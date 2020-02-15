import dumpv2
import json
from common import DIR

import websocket

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
    return dumpv2.WebSocketDumper(DIR, 'bitmex', 'wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,' \
               'instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade', None, channel_analyzer)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()