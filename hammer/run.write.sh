java \
-ea \
-server \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=20 \
-XX:InitiatingHeapOccupancyPercent=60 \
-jar target/hammer-0.1.0-standalone.jar -c conf/app.edn -m "write"
