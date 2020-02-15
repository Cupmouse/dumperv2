import dumpv2
from common import DIR

import websocket
import urllib.request
import json
import logging

logger = logging.getLogger('Bitfinex')

# number of channels Bitfinex allows to open at maximum
BITFINEX_CHANNEL_LIMIT = 30

def subscribe_gen():
    # before start dumping, bitfinex has too much currencies so it has channel limitation
    # we must cherry pick the best one to observe its trade
    # we can determine this by retrieving trading volumes for each symbol and pick coins which volume is in the most

    logger.info('Retrieving market volumes')

    sub_symbols = None

    request = urllib.request.Request('https://api.bitfinex.com/v2/tickers?symbols=ALL')
    with urllib.request.urlopen(request) as response:
        tickers = json.load(response)

        # take only normal exchange symbol which starts from 't', not funding symbol, 'f'
        # symbol name is located at index 0
        tickers = list(filter(lambda arr: arr[0].startswith('t'), tickers))

        # volume is NOT in USD, example, tETHBTC volume is in BTC
        # must convert it to USD in order to sort them by USD volume
        # for this, let's make a price table
        # last price are located at index 7
        price_table = {arr[0]: arr[7] for arr in tickers}

        # convert raw volume to USD volume
        # tXXXYYY (volume in XXX, price in YYY)
        # if tXXXUSD exist, then volume is (volume of tXXXYYY) * (price of tXXXUSD)
        def usd_mapper(arr):
            # symbol name
            symbol_name = arr[0]
            # raw volume
            volume_raw = arr[8]
            # volume in USD
            volume = 0

            # take XXX of tXXXYYY
            pair_base = arr[0][1:4]

            if 't%sUSD' % pair_base in price_table:
                volume = volume_raw * price_table['t%sUSD' % pair_base]
            else:
                print('could not find proper market to calculate volume for symbol: ' + symbol_name)

            # map to this array format
            return [symbol_name, volume]
        # map using usd_mapper function above
        itr = map(usd_mapper, tickers)
        # now itr (Iterator) has format of
        # [ ['tXXXYYY', 10000], ['tZZZWWW', 20000], ... ]

        # sort iterator by USD volume using sorted().
        # note it requires reverse option, since we are looking for symbols
        # which have the most largest volume
        itr = sorted(itr, key=lambda arr: arr[1], reverse=True)

        # take only symbol, not an object
        itr = map(lambda ticker: ticker[0], itr)

        # trim it down to fit a channel limit
        sub_symbols = list(itr)[:BITFINEX_CHANNEL_LIMIT//2]

    logger.info('Retrieving Done')

    def subscribe(ws: dumpv2.WebSocketDumper):
        subscribe_obj = dict(
            event='subscribe',
            channel=None,
            symbol=None,
        )

        # Subscribe to trades channel
        subscribe_obj['channel'] = 'trades'

        for symbol in sub_symbols:
            subscribe_obj['symbol'] = symbol
            ws.send(json.dumps(subscribe_obj))

        subscribe_obj['channel'] = 'book'

        for symbol in sub_symbols:
            subscribe_obj['symbol'] = symbol
            ws.send(json.dumps(subscribe_obj))

    return subscribe

class BitfinexChannelAnalyzer:
    def __init__(self):
        # map of id versus channel
        self.idvch = dict()

    def send(self, message: str):
        obj = json.loads(message)
        return '%s_%s' % (obj['channel'], obj['symbol'])

    def msg(self, message: str):
        obj = json.loads(message)

        if type(obj) == dict:
            if obj['event'] == 'subscribed':
                # response to subscribe
                event_channel = obj['channel']
                symbol = obj['symbol']
                chanId = obj['chanId']

                self.idvch[chanId] = '%s_%s' % (event_channel, symbol)
                
                return self.idvch[chanId]

            elif obj['event'] == 'info':
                # information message
                return 'info'
            else:
                return dumpv2.CHANNEL_UNKNOWN
        else:
            # obj must be an array
            # normal channel message
            chanId = obj[0]
            return self.idvch[chanId]

def gen():
    subscribe = subscribe_gen()
    channel_analyzer = BitfinexChannelAnalyzer()
    return dumpv2.WebSocketDumper(DIR, 'bitfinex', 'wss://api-pub.bitfinex.com/ws/2', subscribe, channel_analyzer)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()