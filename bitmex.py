import dumpv2
from common import DIR

import websocket

def gen():
    return dumpv2.WebSocketDumper(DIR, 'bitmex', 'wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,' \
               'instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade', None)

def main():
    dumpv2.Reconnecter(gen).do()

if __name__ == '__main__':
    main()