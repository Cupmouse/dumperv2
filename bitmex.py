import dumpv2
import json
from common import DIR

import websocket

class BitmexState:
    def __init__(self):
        self.subscribed = list()
        self.map = dict()

    def send(self, message: str):
        pass

    def msg(self, message: str):
        obj = json.loads(message)

        if 'table' in obj:
            channel = obj['table']
            if channel != 'orderBookL2':
                return channel

            if obj['action'] == 'partial' or obj['action'] == 'insert':
                for elem in obj['data']:
                    key = (elem['symbol'], elem['side'], elem['id'])
                    self.map[key] = { 'price': elem['price'], 'size': elem['size'] }
            elif obj['action'] == 'update':
                for elem in obj['data']:
                    key = (elem['symbol'], elem['side'], elem['id'])
                    before = self.map[key]
                    self.map[key] = { 'price': before['price'], 'size': elem['size'] }
            elif obj['action'] == 'delete':
                for elem in obj['data']:
                    key = (elem['symbol'], elem['side'], elem['id'])
                    del self.map[key]

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
        states.append((dumpv2.CHANNEL_SUBSCRIBED, json.dumps(self.subscribed)))
        data = []
        for (symbol, side, id), elem in self.map.items():
            data.append({ 'symbol': symbol, 'side': side, 'id': id, 'price': elem['price'], 'size': elem['size'] })
        states.append(('orderBookL2', json.dumps(data)))
        return states


def gen():
    state = BitmexState()
    return dumpv2.WebSocketDumper(DIR, 'bitmex', 'wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,' \
               'instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade', None, state)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()