import dumpv2
import json
from common import DIR

import websocket

class BitmexState:
    def __init__(self):
        self.map = dict()

    def msg(self, channel, message: str):
        if channel != 'orderBookL2':
            return

        obj = json.loads(message)
        if 'table' not in obj:
            return
            
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

    def snapshot(self):
        data = []
        for (symbol, side, id), elem in self.map.items():
            data.append({ 'symbol': symbol, 'side': side, 'id': id, 'price': elem['price'], 'size': elem['size'] })
        return [('orderBookL2', json.dumps(data))]


class BitmexChannelAnalyzer:
    def send(self, message: str):
        pass

    def msg(self, message: str):
        obj = json.loads(message)

        if 'table' in obj:
            return obj['table']

        elif 'info' in obj:
            return 'info'

        elif 'subscribe' in obj:
            return obj['subscribe']

        elif 'error' in obj:
            return 'error'

        else:
            return dumpv2.CHANNEL_UNKNOWN

def gen():
    channel_analyzer = BitmexChannelAnalyzer()
    state = BitmexState()
    return dumpv2.WebSocketDumper(DIR, 'bitmex', 'wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,' \
               'instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade', None, channel_analyzer, state)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()