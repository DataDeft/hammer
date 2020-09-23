(ns hammer.core
  (:require
    [hammer.utils           :refer :all             ]
    [clojure.core.async     :as async               ]
    [clojure.tools.logging  :as log                 ]
    )
  ; Java
  (:import
    [java.util                                        Arrays UUID               ]
    [java.net                                         InetSocketAddress         ]
    [clojure.core.async.impl.channels                 ManyToManyChannel         ]
    [com.datastax.oss.driver.api.core                 CqlSession CqlIdentifier  ]
    [com.datastax.oss.driver.api.querybuilder         SchemaBuilder QueryBuilder]
    [com.datastax.oss.driver.api.core.type            DataTypes                 ]
    [com.datastax.oss.driver.api.core.metadata.schema ClusteringOrder           ]
    )
  (:gen-class))

(defn getSession
  [host port datacenter]
  (try
    (let
      [
       contactPoint (InetSocketAddress. host port)
      ]
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
    (let
      [
       contactPoint (InetSocketAddress. host port)
      ]
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
  (for [host (.values (.getNodes (.getMetadata session)))]
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
  (let [statement (->
                    (SchemaBuilder/createKeyspace keyspaceName)
                    (.ifNotExists)
                    (.withNetworkTopologyStrategy replicationMap)
                    (.withDurableWrites durableWrites)
                    (.build)) ]
    ; creating keyspace
    (.execute session statement)))

(defn getExecutedStatement
  [executed-statement]
  (.getQuery
    (.getStatement
      (.getExecutionInfo executed-statement))))

(defn createTable0
  [session]
  (let [statement
        (->
          (SchemaBuilder/createTable "table0")
          (.ifNotExists)
          (.withPartitionKey            "userId"    DataTypes/UUID)
          (.withClusteringColumn        "deviceId"  DataTypes/UUID)
          (.withColumn                  "hash"      DataTypes/TEXT)
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

(defn insertIntoTable0
  [session userid deviceid hash]
  (let [ statement
          (->
            (QueryBuilder/insertInto "table0")
            (.value "userid" (QueryBuilder/literal userid))
            (.value "deviceid" (QueryBuilder/literal deviceid))
            (.value "hash" (QueryBuilder/literal hash))
            (.build)) ]
    (.execute session statement)))

(defn -main
  [& args]
  (let
    [
      opts          (getOpts args cli-options)
      configMaybe   (read-config (get-in opts [:options :config]))
    ]
    (log/info "Starting up...")
    (if (:ok configMaybe)
      (log/info configMaybe)
      (do
        (log/error configMaybe)
        (exit 1)))
    (try
      (let
        [
          config              (:ok configMaybe)
          host                (get-in config [:cassandra-client :initial-server-host])
          port                (get-in config [:cassandra-client :initial-server-port])
          keyspace            (get-in config [:cassandra-client :keyspace])
          dc                  (get-in config [:cassandra-client :dc])
          replication         (get-in config [:cassandra-client :replication-factor])
          durable-writes      (get-in config [:cassandra-client :durable-writes])
          iterations          (get-in config [:hammer :number-of-iterations])
          runs                (get-in config [:hammer :number-of-runs])
          _                   (log/info "Connecting to cluster")
          initialSessionMaybe (getSession host port dc)
        ]

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

        ; try to connect to the testing keyspace previously created
        (let [keyspacedSessionMaybe (getSessionWithKeyspace host port dc keyspace)]
          (if (:ok keyspacedSessionMaybe)
            (let [keyspacedSession (:ok keyspacedSessionMaybe)]
              (do
                (log/info
                  (format "Connected to %s" keyspace))
                (log/info
                  (getExecutedStatement (createTable0 keyspacedSession)))

                (dotimes [r runs]
                  (log/info (format "Starting run: %s" r))
                  (let [^"[D" perf (make-array Double/TYPE iterations)]
                    (dotimes [n iterations]
                      (let
                        [
                          start     (System/nanoTime) ; nanoseconds
                          userId    (UUID/randomUUID)
                          deviceId  (UUID/randomUUID)
                          hash      (rand-str 5000)
                        ]
                        (insertIntoTable0 keyspacedSession userId deviceId hash)
                        (aset perf n (deltaTimeMs start (System/nanoTime)))))
                      (log/info "Printing stats")
                      (let
                        [
                          _           (Arrays/sort perf)
                          totalTime   (.floatValue (areduce perf i ret 0 (+ ret (aget perf i))))
                          performance (.intValue (* (/ iterations totalTime) iterations))
                          p50         (aget perf (.intValue (* iterations 0.5)))
                          p90         (aget perf (.intValue (* iterations 0.9)))
                          p99         (aget perf (.intValue (* iterations 0.99)))
                          p999        (aget perf (.intValue (* iterations 0.999)))
                          p100        (aget perf (- iterations 1))
                        ]
                        (log/info (format "Finished %s iterations" iterations))
                        (log/info (format "Total time %.2f" totalTime))
                        (log/info (format "Performance %s ops/s" performance))
                        (log/info (format "Percentiles :: p50 = %.1fms : p90 = %.1fms : p99 = %.1fms : p999 = %.1fms : p100 = %.1fms" p50 p90 p99 p999 p100))
                        )))
                (exit 0)))
            (do
              (log/error (format "Could not connect to %s" keyspace))
              (exit 1)))))

     (catch Exception e
      (do
        (log/error (str "caught exception: " (.getMessage e)))
        (log/error e)
        (exit 1))))))
