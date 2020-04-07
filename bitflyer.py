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
        # idvch is map of subscribe request id vs channel name
        self.idvch = dict()
        # subscribed list contains only channels that got subscribed message from server
        # idvch could contain channels that never got approved from server
        self.subscribed = list()
        # orderbook map[channel][asks/bids][price]size
        self.map = dict()

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
                channel = obj['params']['channel']

                if not channel.startswith('lightning_board_snapshot_') and not channel.startswith('lightning_board_'):
                    return channel

                if channel.startswith('lightning_board_snapshot_'):
                    # this is the partial orderbook, don't be confused, this is not a complete snapshot
                    memOrderbook = self.map[channel] = dict()
                    memAsks = memOrderbook['asks'] = dict()
                    memBids = memOrderbook['bids'] = dict()
                elif channel.startswith('lightning_board_'):
                    # orderbook change apply it to current orderbook in memory
                    if channel not in self.map:
                        # bitflyer sends board change even if snapshot is not yet given ignore this
                        return
                    memOrderbook = self.map[channel]
                    memAsks = memOrderbook['asks']
                    memBids = memOrderbook['bids']

                msgObj = obj['params']['message']

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

                return channel
            else:
                return dumpv2.CHANNEL_UNKNOWN
        else:
            # response to subscribe
            channel = self.idvch[obj['id']]
            self.subscribed.append(channel)
            return channel

    # snapshot contains
    # CHANNEL_SUBSCRIBED: an array of names of subscribed channels
    # lightning_board_snapshot_[symbol]: generated snapshot of orderbooks of [symbol]
    #   in the same format with "message" attribute in raw data
    def snapshot(self):
        states = []

        states.append((dumpv2.CHANNEL_SUBSCRIBED, json.dumps(self.subscribed)))

        for channel, orderbook in self.map.items():
            symbol = ""
            if channel.startswith("lightning_board_snapshot_"):
                symbol = channel[len("lightning_board_snapshot_"):]
            else:
                symbol = channel[len("lightning_board_"):]
            message = dict()
            message['asks'] = [ { 'price': price, 'size': size } for price, size in sorted(orderbook['asks'].items()) ]
            message['bids'] = [ { 'price': price, 'size': size } for price, size in sorted(orderbook['bids'].items(), reverse=True) ]
            states.append(('lightning_board_snapshot_%s' % symbol, json.dumps(message)))
        return states


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
    state = BitflyerState()
    return dumpv2.WebSocketDumper(DIR, 'bitflyer', 'wss://ws.lightstream.bitflyer.com/json-rpc', subscribe, state)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()