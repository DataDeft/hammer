# hammer

I thought it will a simple performance tool for Cassandra, running simple workloads as loadtest. It ended up being a complete rabbithole investigation into the internals of the Cassandra driver. Still no luck with performance. It is somewhat slow.

## Usage

```bash
cat run.sh
java \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.port=9010 \
-Dcom.sun.management.jmxremote.rmi.port=9011 \
-Djava.rmi.server.hostname=localhost \
-Dcom.sun.management.jmxremote.local.only=false \
-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=20 \
-XX:InitiatingHeapOccupancyPercent=60 \
-jar target/hammer-0.1.0-standalone.jar -c conf/app.edn
```

```
bash run.sh
```

## Flame

![Flame](./flame.svg)


## License

Copyright <2020> <LambdaInsight SARL>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


