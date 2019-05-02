

# Based on the example provided by pubnub


import time
import sys

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

pnconfig = PNConfiguration()

pnconfig.subscribe_key = 'sub-c-5f1b7c8e-fbee-11e3-aa40-02ee2ddab7fe'
pnconfig.publish_key = 'demo'

channel_name = 'pubnub-sensor-network'
pubnub = PubNub(pnconfig)


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, file=sys.stderr)


class MySubscribeCallback(SubscribeCallback):
    def __init__(self):
        self._counter = 0
        self._start_time = None
        self._window_start_time = None
        self._window_size = 10

    def presence(self, pubnub, presence):
        log("presence")
        # handle incoming presence data

    def status(self, pubnub, status):
        log("status")
        if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
            pass  # This event happens when radio / connectivity is lost
        elif status.category == PNStatusCategory.PNConnectedCategory:
            self._start_time = time.time()
            self._window_start_time = self._start_time
            log("Connected!!")
            # Connect event. You can do stuff like publish, and know you'll get it.
            # Or just use the connected event to confirm you are subscribed for
            # UI / internal notifications, etc
            # pubnub.publish().channel("awesomeChannel").message("hello!!").pn_async(my_publish_callback)
        elif status.category == PNStatusCategory.PNReconnectedCategory:
            pass
            # Happens as part of our regular operation. This event happens when
            # radio / connectivity is lost, then regained.
        elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
            pass
            # Handle message decryption error. Probably client configured to
            # encrypt messages and on live data feed it received plain text.

    def message(self, pubnub, message):
        # Handle new message stored in message.message
        self._counter += 1
        print(message.message)
        if self._counter % self._window_size == 0:
            now = time.time()
            avg_rate = self._counter / (now - self._start_time)
            window_rate = self._window_size / (now - self._window_start_time)
            log("Avg message rate: {:0.1f} msg/sec; Last {} rate: {:0.1f} msg/sec".format(avg_rate, self._window_size, window_rate))
            self._window_start_time = now


if __name__ == '__main__':
    my_listener = MySubscribeCallback()
    pubnub.add_listener(my_listener)
    pubnub.subscribe().channels(channel_name).execute()
