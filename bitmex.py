import dumpv2
import json
from common import DIR

import websocket

class BitmexState:
    def __init__(self):
        self.subscribed = list()
        self.orderbooks = dict()
        self.instruments = dict()

    def send(self, message: str):
        pass

    def msg(self, message: str):
        obj = json.loads(message)

        if 'table' in obj:
            channel = obj['table']

            if channel == 'orderBookL2':
                if obj['action'] == 'partial' or obj['action'] == 'insert':
                    # orderbook snapshot
                    for elem in obj['data']:
                        dueToRemove = set()
                        if elem['side'] == 'Sell':
                            for memKey in self.orderbooks:
                                if memKey[0] == elem['symbol'] \
                                    and memKey[1] == 'Buy' \
                                    and self.orderbooks[memKey]['price'] >= elem['price']:
                                    dueToRemove.add(memKey)
                        else:
                            for memKey in self.orderbooks:
                                if memKey[0] == elem['symbol'] \
                                    and memKey[1] == 'Sell' \
                                    and self.orderbooks[memKey]['price'] <= elem['price']:
                                    dueToRemove.add(memKey)

                        for remove in dueToRemove:
                            print(remove, self.orderbooks[remove])
                            del self.orderbooks[remove]

                        key = (elem['symbol'], elem['side'], elem['id'])
                        self.orderbooks[key] = { 'price': elem['price'], 'size': elem['size'] }

                elif obj['action'] == 'update':
                    # update order size
                    for elem in obj['data']:
                        key = (elem['symbol'], elem['side'], elem['id'])
                        if key not in self.orderbooks:
                            # key is not in the orderbook
                            continue
                        before = self.orderbooks[key]
                        self.orderbooks[key] = { 'price': before['price'], 'size': elem['size'] }
                elif obj['action'] == 'delete':
                    # delete order from orderbook
                    for elem in obj['data']:
                        key = (elem['symbol'], elem['side'], elem['id'])
                        if key in self.orderbooks:
                            del self.orderbooks[key]
            elif channel == 'instrument':
                if obj['action'] == 'partial':
                    for elem in obj['data']:
                        # just store the whole element to dict key as symbol
                        self.instruments[elem['symbol']] = elem
                elif obj['action'] == 'update':
                    for elem in obj['data']:
                        # update value of element of instrument dict
                        # do not replace the entire object
                        for key, value in elem.items():
                            # sometimes, bitmex does not send insert/partial for instruemnts
                            if elem['symbol'] in self.instruments:
                                self.instruments[elem['symbol']][key] = value

            return channel

        elif 'info' in obj:
            return 'info'

        elif 'subscribe' in obj:
            # response to subscribe
            channel = obj['subscribe']
            self.subscribed.append(channel)
            return channel

        elif 'error' in obj:
            return 'error'

        else:
            return dumpv2.CHANNEL_UNKNOWN

    def snapshot(self):
        states = []
        # subscribed snapshot
        states.append((dumpv2.CHANNEL_SUBSCRIBED, json.dumps(self.subscribed)))
        # orderbook snapshot
        data = []
        for (symbol, side, id), elem in self.orderbooks.items():
            data.append({ 'symbol': symbol, 'side': side, 'id': id, 'price': elem['price'], 'size': elem['size'] })
        states.append(('orderBookL2', json.dumps(data)))

        # instrument snapshot
        # symbol key of dict is only for updating
        # symbol is also included in the value of dict
        states.append(('instrument', json.dumps(list(self.instruments.values()))))
        return states


def gen():
    state = BitmexState()
    return dumpv2.WebSocketDumper(DIR, 'bitmex', 'wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,' \
               'instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade', None, state)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()