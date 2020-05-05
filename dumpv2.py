from enum import Enum

import os
import websocket
import gzip
import time
import datetime
import logging
import threading
import queue
import sys

CHANNEL_UNKNOWN = '!unknown'
CHANNEL_SUBSCRIBED = "!subscribed"

class Writer():
    def __init__(self, directory: str, prefix: str, url: str, state):
        self.directory = directory
        self.prefix = prefix
        self.url = url
        # analyze message and returns what channel it is
        self.state = state
        
        self.stream = None
        self.min_opened = None
        self.last_time = 0

        self.logger = logging.getLogger('writer')
        self.closed = False

    def no_time_backwards(self, time: int):
        # prevent time from going backwards
        if time < self.last_time:
            self.logger.warn('time is going backwards??!!!')
            return self.last_time
            # no last_time update
        else:
            # update last_time
            self.last_time = time
            return time

    def open(self, time: int):
        if self.closed:
            self.logger.error('already closed')
            return

        # convert from nanosec to sec
        time_sec = time // 1_000_000_000
        # calculate mins in unixtime
        time_min = time_sec // 60

        if (self.stream == None) or (self.min_opened != time_min):
            # if this is the first time
            # or
            # make new file each minute (to cut seeking time on read) 

            # this shows if this is the first time
            is_first_time = self.stream == None

            # close previous file
            if not is_first_time:
                self.stream.flush()
                self.stream.close()

            self.logger.info('making new file')

            # this is the first time, open new file
            file_path = os.path.join(self.directory, '%s_%d.gz' % (self.prefix, time))
            
            # make directories if not exist
            if not os.path.exists(self.directory):
                os.makedirs(self.directory)

            # open gzip stream
            self.stream = gzip.open(file_path, 'at')

            # record the time opened
            self.min_opened = time_min

            if is_first_time:
                # write start line
                self.stream.write('start\t%d\t%s\n' % (time, self.url))
            else:
                if time_min % 10 == 0:
                    # if this is not the first file and first digit of minute is 0, then
                    # write state snapshot
                    snapshot = self.state.snapshot()
                    for (channel, state) in snapshot:
                        self.stream.writelines(['state\t%s\t%s\t' % (time, channel), state, '\n'])

    """write message"""
    def msg(self, msg: str, time: int):
        time = self.no_time_backwards(time)
        self.open(time)
        # get channel name
        try:
            channel = self.state.msg(msg)
        except Exception:
            self.logger.exception('channel analyzer failed %s', msg)
            channel = CHANNEL_UNKNOWN
        if channel is None:
            channel = CHANNEL_UNKNOWN
        if channel == CHANNEL_UNKNOWN:
            self.logger.warning('unknown channel detected %s', msg)
        # write a line
        self.stream.writelines(['msg\t%d\t%s\t' % (time, channel), msg, '\n'])

    def send(self, msg: str, time: int):
        time = self.no_time_backwards(time)
        self.open(time)
        # get channel name
        try:
            channel = self.state.send(msg)
        except Exception:
            self.logger.exception('channel analyzer failed %s', msg)
            channel = CHANNEL_UNKNOWN
        if channel is None:
            channel = CHANNEL_UNKNOWN
        if channel == CHANNEL_UNKNOWN:
            self.logger.warning('unknown channel detected %s', msg)
        # write a line
        self.stream.writelines(['send\t%d\t%s\t' % (time, channel), msg, '\n'])

    def err(self, msg: str, time: int):
        time = self.no_time_backwards(time)
        self.open(time)
        self.stream.writelines(['err\t%d\t' % time, msg, '\n'])

    def end(self, time: int):
        time = self.no_time_backwards(time)
        self.open(time)
        # write a line
        self.stream.write('end\t%s\n' % time)
        
        self.closed = True
        self.stream.flush()
        self.stream.close()

class MultithreadedWriter(threading.Thread):
    def __init__(self, directory: str, prefix: str, url: str, state):
        super().__init__()
        self.writer = Writer(directory, prefix, url, state)
        self.queue = queue.Queue()
        self.exception = None

    # this runs in the different thread
    def run_with_exception(self):
        while True:
            # this will block if there is no item in the queue until it become available
            item = self.queue.get()
            if item['type'] == "open":
                self.writer.open(item['time'])
            elif item['type'] == "msg":
                self.writer.msg(item['msg'], item['time'])
            elif item['type'] == "send":
                self.writer.send(item['msg'], item['time'])
            elif item['type'] == "err":
                self.writer.err(item['msg'], item['time'])
            elif item['type'] == "end":
                self.writer.end(item['time'])
                break # end this thread
            self.queue.task_done()

    # this runs in the different thread
    def run(self):
        try:
            # Possibly throws an exception
            self.run_with_exception()
        except Exception as e:
            self.exception = e

    def open(self, time: int):
        if self.exception != None: raise Exception("Error occurred on writer thread") from self.exception
        self.queue.put({ 'type': "open", 'time': time })

    def msg(self, msg: str, time: int):
        if self.exception != None: raise Exception("Error occurred on writer thread") from self.exception
        self.queue.put({ 'type': "msg", 'msg': msg,  'time': time })

    def send(self, msg: str, time: int):
        if self.exception != None: raise Exception("Error occurred on writer thread") from self.exception
        self.queue.put({ 'type': "send", 'msg': msg, 'time': time })

    def err(self, msg: str, time: int):
        if self.exception != None: raise Exception("Error occurred on writer thread") from self.exception
        self.queue.put({ 'type': "err", 'msg': msg, 'time': time })

    def end(self, time: int):
        if self.exception != None: raise Exception("Error occurred on writer thread") from self.exception
        self.queue.put({ 'type': "end", 'time': time })

"""dump WebSocket stream"""
class WebSocketDumper:
    def __init__(self, dir_dump: str, exchange: str, url: str, subscribe, state):
        self.url = url
        # called when connected
        self.subscribe = subscribe
        
        # create new writer for this dumper
        self.writer = MultithreadedWriter(os.path.join(dir_dump, exchange), exchange, url, state)
        # WebSocketApp for serving WebSocket stream
        self.ws_app = None
        
        self.logger = logging.getLogger('websocket')

    def send(self, message: str):
        self.ws_app.send(message)
        timestamp = time.time_ns()
        try:
            self.writer.send(message, timestamp)
        except Exception:
            self.logger.exception('error on send')
            self.ws_app.close()

    def do(self):
        self.logger.info('Connecting to [%s]...' % self.url)

        # listeners
        def on_close(ws):
            self.logger.warn('WebSocket closed for [%s]' % self.url)
            self.writer.end(time.time_ns())

        def on_message(ws, message):
            timestamp = time.time_ns()
            try:
                self.writer.msg(message, timestamp)
            except Exception:
                self.logger.exception("writer msg returned error")
                ws.close()


        def on_error(ws, error):
            timestamp = time.time_ns()
            self.logger.error('Got WebSocket error [%s]:' % self.url)
            self.logger.error(error)
            self.writer.err(str(error), timestamp)

            try:
                ws.close()
            except Exception:
                self.logger.exception('ws.close() failed')

        def on_open(ws):
            self.logger.info('WebSocket opened for [%s]' % self.url)

            if self.subscribe != None:
                try:
                    # Do subscribing process
                    self.subscribe(self)
                except Exception:
                    self.logger.exception('Encountered an error on subscribe')
                    ws.close()

        # start the writer thread
        self.writer.start()
        # Open connection to target WebSocket server
        self.ws_app = websocket.WebSocketApp(self.url,
                                                on_open=on_open,
                                                on_message=on_message,
                                                on_error=on_error,
                                                on_close=on_close)

        try:
            self.ws_app.run_forever()
        except KeyboardInterrupt as e:
            self.logger.warn('Got kill command, ending stream')
            self.writer.end(time.time_ns())
            raise e

class Reconnecter:
    DEFAULT_RECONNECTION_TIME = 1  # default wait time is 1 second
    MAX_RECONNECTION_TIME = 60  # reconnection time will not be more than this value

    def __init__(self, gen_dump):
        self.gen_dump = gen_dump

        self.logger = logging.getLogger('reconnector')

    def do(self):
        # seconds to wait
        time_wait = self.DEFAULT_RECONNECTION_TIME
        # last connection time
        time_connect = None

        while True:
            time_connect = datetime.datetime.utcnow()

            try:
                self.gen_dump().do()
            except KeyboardInterrupt as e:
                raise e
            except Exception:
                self.logger.exception('uncatched error in dumper')

            # wait seconds not to dead loop
            # wait if disconnected in less than 5 minites from connection
            if ((datetime.datetime.utcnow() - time_connect) / datetime.timedelta(minutes=1) <= 5):
                # wait
                self.logger.warn('Waiting %d seconds...' % time_wait)
                time.sleep(time_wait)

                # set wait time as twice as the time before
                time_wait = min(time_wait*2, self.MAX_RECONNECTION_TIME)
            else:
                # reset wait time and do not wait
                time_wait = self.DEFAULT_RECONNECTION_TIME
