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
    return dumpv2.WebSocketDumper(DIR, 'bitflyer', 'wss://ws.lightstream.bitflyer.com/json-rpc', subscribe)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()