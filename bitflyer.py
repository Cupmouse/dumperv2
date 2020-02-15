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

    with urllib.request.urlopen(request) as response:
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
    return dumpv2.WebSocketDumper(DIR, 'bitflyer', 'wss://ws.lightstream.bitflyer.com/json-rpc', subscribe, channel_analyzer)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()