#!/bin/sh
mkdir dumperv2
if cp common.py dumpv2.py bitfinex.py bitmex.py bitflyer.py dumperv2 ; then
  rm dumperv2.zip
  zip dumperv2.zip -r dumperv2
  rm -r dumperv2
else
  rmdir dumperv2
fi
