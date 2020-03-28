import dumpv2
from common import DIR

import websocket
import urllib.request
import json

# prefixes for individual channel
BITFLYER_CHANNEL_PREFIXES = [
    'lightning_executions_',
    'lightning_board_snapshot_',
    'lightning_board_',
    'lightning_ticker_',
]

class BitflyerState:
    def __init__(self):
        self.map = dict()

    def msg(self, channel: str, message: str):
        if not channel.startswith('lightning_board_snapshot_') and not channel.startswith('lightning_board_'):
            return
        obj = json.loads(message)
        if 'method' not in obj:
            return
        msgObj = obj['params']['message']

        if channel.startswith('lightning_board_snapshot_'):
            # this is the partial orderbook, don't be confused, this is not a complete snapshot
            symbol = channel[len('lightning_board_snapshot_'):]
            memOrderbook = self.map[symbol] = dict()
            memOrderbook['asks'] = dict()
            memOrderbook['bids'] = dict()

            memOrderbook = self.map[symbol]
            memAsks = memOrderbook['asks']
            memBids = memOrderbook['bids']
            for ask in msgObj['asks']:
                memAsks[ask['price']] = ask['size']
            for bid in msgObj['bids']:
                memBids[bid['price']] = bid['size']

        elif channel.startswith('lightning_board_'):
            # orderbook change apply it to current orderbook in memory
            symbol = channel[len('lightning_board_'):]
            if symbol not in self.map:
                # bitflyer sends board change even if snapshot is not yet given ignore this
                return
            memOrderbook = self.map[symbol]
            memAsks = memOrderbook['asks']
            memBids = memOrderbook['bids']
            for ask in msgObj['asks']:
                if ask['price'] == 0:
                    # itayose market order execution, ignore
                    continue
                if ask['size'] == 0:
                    # order was canceled and slot became empty
                    if ask['price'] in memAsks:
                        del memAsks[ask['price']]
                else:
                    memAsks[ask['price']] = ask['size']
            for bid in msgObj['bids']:
                if bid['price'] == 0: continue
                if bid['size'] == 0:
                    if bid['price'] in memBids:
                        del memBids[bid['price']]
                else: memBids[bid['price']] = bid['size']

    def snapshot(self):
        lines = []
        for symbol, orderbook in self.map.items():
            message = dict()
            message['asks'] = [ { 'price': price, 'size': size } for price, size in sorted(orderbook['asks'].items()) ]
            message['bids'] = [ { 'price': price, 'size': size } for price, size in sorted(orderbook['bids'].items(), reverse=True) ]
            lines.append(('lightning_board_snapshot_%s' % symbol, json.dumps(message)))
        return lines



class BitflyerChannelAnalyzer:
    def __init__(self):
        self.idvch = dict()

    def send(self, message: str):
        obj = json.loads(message)
        # id is rpc id, response to this only contains id so we need this later
        self.idvch[obj['id']] = obj['params']['channel']

        return self.idvch[obj['id']]

    def msg(self, message: str):
        obj = json.loads(message)

        if 'method' in obj:
            if obj['method'] == 'channelMessage':
                # normal channel message
                return obj['params']['channel']
            else:
                return dumpv2.CHANNEL_UNKNOWN
        else:
            # response to subscribe
            return self.idvch[obj['id']]

def subscribe_gen():
    product_codes = None

    # get markets
    request = urllib.request.Request('https://api.bitflyer.com/v1/markets')

    with urllib.request.urlopen(request, timeout=1) as response:
        markets = json.load(response)

        # response is like [{'product_code':'BTC_JPY'},{...}...]
        # convert it to an array of 'product_code'
        product_codes = [obj['product_code'] for obj in markets]

    def subscribe(ws: dumpv2.WebSocketDumper):
        # sending subscribe call to the server
        subscribe_obj = dict(
            method='subscribe',
            params=dict(
                channel=None
            ),
            id=None,
        )

        # send subscribe message
        curr_id = 1
        for product_code in product_codes:
            for prefix in BITFLYER_CHANNEL_PREFIXES:
                subscribe_obj['params']['channel'] = prefix + product_code
                subscribe_obj['id'] = curr_id
                curr_id += 1
                ws.send(json.dumps(subscribe_obj))

    return subscribe

def gen():
    subscribe = subscribe_gen()
    channel_analyzer = BitflyerChannelAnalyzer()
    state = BitflyerState()
    return dumpv2.WebSocketDumper(DIR, 'bitflyer', 'wss://ws.lightstream.bitflyer.com/json-rpc', subscribe, channel_analyzer, state)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()