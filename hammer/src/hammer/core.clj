(ns hammer.core
  (:require
    [hammer.utils       :refer [exit read-config getOpts
                                cli-options rand-str rand-str2]]
    [clojure.core.async :as as     ]
    [clojure.tools.logging :as log ]
    )
  ; Java
  (:import
    [java.util                                        Arrays UUID               ]
    [java.net                                         InetSocketAddress         ]
    [java.nio                                         ByteBuffer                ]
    [com.datastax.oss.driver.api.core                 CqlSession CqlIdentifier
                                                      DefaultConsistencyLevel   ]
    [com.datastax.oss.driver.api.querybuilder         SchemaBuilder QueryBuilder]
    [com.datastax.oss.driver.api.core.type            DataTypes                 ]
    [com.datastax.oss.driver.api.core.metadata.schema ClusteringOrder           ]
    )
  (:gen-class))

(defn getSession
  [host port datacenter]
  (try
    (let [contactPoint (InetSocketAddress. host port)]
      {:ok
        (->
          (CqlSession/builder)
          (.addContactPoint contactPoint)
          (.withLocalDatacenter datacenter)
          (.build))})
  (catch Exception e
    {:error "Exception" :fn "getSession" :exception (.getMessage e)})))

(defn getSessionWithKeyspace
  [host port datacenter keyspace]
  (try
    (let [ contactPoint (InetSocketAddress. host port) ]
      {:ok
       (->
        (CqlSession/builder)
        (.addContactPoint contactPoint)
        (.withLocalDatacenter datacenter)
        (.withKeyspace (CqlIdentifier/fromCql keyspace))
        (.build))})
  (catch Exception e
    {:error "Exception" :fn "getSession" :exception (.getMessage e)})))

(defn getMetaData
  [session]
  (for [host (.values (.getNodes (.getMetadata session)))] ; session.getMetada.getNodes.values()
    (str
      (.getDatacenter host)
      " : " (.getRack host)
      " : " (.toString (.getEndPoint host))
      " : " (.getState host)
      " : " (.toString (.getCassandraVersion host))
      " : " (.getOpenConnections host))))

(defn getKeyspaces
  [session]
  (for [keyspace (.values (.getKeyspaces (.getMetadata session)))]
    (.toString (.getName keyspace))))

(defn createKeyspace
  ;(createKeyspace "test" {"dc1" 3} true)
  [session keyspaceName replicationMap durableWrites]
  (let [ statement
         (->
          (SchemaBuilder/createKeyspace keyspaceName)
          (.ifNotExists)
          (.withNetworkTopologyStrategy replicationMap)
          (.withDurableWrites durableWrites)
          (.build))]
    ; creating keyspace
    (.execute session statement)))

(defn getExecutedStatement
  [executed-statement]
  (.getQuery
    (.getStatement
      (.getExecutionInfo executed-statement))))

(defn createTable0
  [session]
  (let [ statement
         (->
          (SchemaBuilder/createTable "table0")
          (.ifNotExists)
          (.withPartitionKey            "userId"    DataTypes/UUID)
          (.withClusteringColumn        "deviceId"  DataTypes/UUID)
          (.withColumn                  "hash"      DataTypes/TEXT)
          (.withColumn                  "bob"       DataTypes/BLOB)
          (.withClusteringOrder         "deviceId"  ClusteringOrder/ASC)
          (.withLZ4Compression          64 1.0)
          (.withMemtableFlushPeriodInMs 1024)
          (.build)
          )]
    (.execute session statement)))

(defn logClusterInfo
  [session]
  (log/info " :: Datacenter : Rack : Host+Port : State : # of connections")
  (doseq
    [m (getMetaData session)]
    (log/info (str "Node :: " m)))
  (doseq
    [k (getKeyspaces session)]
    (log/info (str "Keyspace :: " k))) )

(defn deltaTimeMs
  [start end]
  (/ (- end start) 1000000.0))

(defn f
  [m]
  (format "%.1fms" m))

(defn getConfig
  [args]
  (let
    [ opts          (getOpts args cli-options)
      configMaybe   (read-config (get-in opts [:options :config])) ]
    (log/info "Starting up...")
    (if (:ok configMaybe)
      (do
        (log/info configMaybe)
        configMaybe)
      (do
        (log/error configMaybe)
        (exit 1)))))

(defn insertIntoTable0NoBind
  [session userid deviceid hash bob]
  (let [ statement
          (->
            (QueryBuilder/insertInto "table0")
            (.value "userid"    (QueryBuilder/literal userid))
            (.value "deviceid"  (QueryBuilder/literal deviceid))
            (.value "hash"      (QueryBuilder/literal hash))
            (.value "bob"       bob)
            (.build)) ]
    (.execute session statement)))

(defn getInsertStatementBind 
  []
  (->
   (QueryBuilder/insertInto "table0")
   (.value "userid"    (QueryBuilder/bindMarker))
   (.value "deviceid"  (QueryBuilder/bindMarker))
   (.value "hash"      (QueryBuilder/bindMarker))
   (.value "bob"       (QueryBuilder/bindMarker))
   (.build)
   (.setConsistencyLevel (DefaultConsistencyLevel/QUORUM))))

(defn insertIntoTable0
  [session userid deviceid hash bob]
  (let [statement (getInsertStatementBind)
        prepared  (.prepare session statement)
        bound     (.bind prepared (into-array Object [userid deviceid hash (ByteBuffer/wrap bob)]))]
    (.execute session bound)))

(defn insertTaskOneSession
  [session runs iterations stat-chan]
  (dotimes [r runs]
    (try
      (let [^"[D" perf (make-array Double/TYPE iterations)]
        (log/info (format "Starting run: %s in thread: %s" r (.getName (Thread/currentThread))))
        ; starting run
        (dotimes [n iterations]
          (let [start     (System/nanoTime) ; nanoseconds
                userId    (UUID/randomUUID)
                deviceId  (UUID/randomUUID)
                hash      (rand-str2 5000)
                bob       (.getBytes hash)]
            (insertIntoTable0 session userId deviceId hash bob)
            (aset perf n (deltaTimeMs start (System/nanoTime)))))
        ; end run
        (log/info (format "Finished run: %s in thread: %s" r (.getName (Thread/currentThread))))
        (as/>!!
          stat-chan
          {:thread-name (.getName (Thread/currentThread)) :run r :perf perf}))
      ; return
      {:ok :ok}
    (catch Exception e
      (log/error (str "caught exception: " (.getMessage e)))
      (log/error e)
      {:err :err}))))

(defn -main
  [& args]
  (try
    (let [ configMaybe         (getConfig args)
           config              (:ok configMaybe)
           host                (get-in config [:cassandra-client :initial-server-host])
           port                (get-in config [:cassandra-client :initial-server-port])
           keyspace            (get-in config [:cassandra-client :keyspace])
           dc                  (get-in config [:cassandra-client :dc])
           replication         (get-in config [:cassandra-client :replication-factor])
           durable-writes      (get-in config [:cassandra-client :durable-writes])
           iterations          (get-in config [:hammer :number-of-iterations])
           runs                (get-in config [:hammer :number-of-runs])
           channel-timeout     (get-in config [:hammer :channel-timeout])
           thread-count        (get-in config [:hammer :thread-count])
           _                   (log/info "Connecting to cluster")
           initialSessionMaybe (getSession host port dc)]
      ; creating keyspace
      (if (:ok initialSessionMaybe)
        ; ok
        (let [initial-session (:ok initialSessionMaybe)]
          (logClusterInfo initial-session)
          (log/info
            (getExecutedStatement
              (createKeyspace initial-session keyspace {dc replication} durable-writes))))
        ; err
        (do
          (log/error "Initial Cassandra session could not be established")
          (log/error initialSessionMaybe)
          (exit 1)))
      ; creating threads and running inserts
      (let
        [ stat-chan (as/chan 8)
         runResults (atom {})
         insertSession (:ok (getSessionWithKeyspace host port dc keyspace)) ]
        ; worker threads
        (dotimes [_ thread-count]
          (as/thread
            (Thread/sleep 100)
            (insertTaskOneSession insertSession runs iterations stat-chan)))

        ; main thread
        (while true
          (as/<!!
            (as/go
              (let [[msg source] (as/alts! [stat-chan (as/timeout channel-timeout)])]
                (if (= source stat-chan)
                  (let [ msg-run (keyword (.toString (:run msg))) ]
                    (swap! runResults update-in [msg-run] conj (:perf msg))
                    (if  (= thread-count (count (get-in @runResults [msg-run])))
                      (let [
                          perf        (double-array (apply concat (msg-run @runResults)))
                          _           (Arrays/sort perf)
                          totalTime   (.floatValue (areduce perf i ret 0 (+ ret (aget perf i))))
                          performance (.intValue (* (/ (* thread-count iterations) totalTime) 1000))
                          p50         (aget perf (.intValue (* (* thread-count iterations) 0.5)))
                          p90         (aget perf (.intValue (* (* thread-count iterations) 0.9)))
                          p99         (aget perf (.intValue (* (* thread-count iterations) 0.99)))
                          p999        (aget perf (.intValue (* (* thread-count iterations) 0.999)))
                          p100        (aget perf (- (* thread-count iterations) 1))
                        ]
                        (swap! runResults assoc-in [msg-run] {:totalTime (f totalTime) :performance performance :percentiles {:p50 (f p50) :p90 (f p90) :p99 (f p99) :999 (f p999) :p100 (f p100)}})
                        (log/info @runResults))
                      ; else
                      (log/debug "Got message from a thread but the measurement is not complete yet")))
                  ;else - timeout
                  (do
                    (log/info "Channel timed out. Stopping...")
                    (exit 0)))))))))
    (catch Exception e
      (log/error (str "caught exception: " (.getMessage e)))
      (log/error e)
      (exit 1))))
