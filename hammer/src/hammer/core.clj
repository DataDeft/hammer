(ns hammer.core
  (:require
    [hammer.utils           :refer :all             ]
    [clojure.core.async     :as async               ]
    [clojure.tools.logging  :as log                 ]
    )
  ; Java
  (:import
    [java.net                                         InetSocketAddress    ]
    [clojure.core.async.impl.channels                 ManyToManyChannel    ]
    [com.datastax.oss.driver.api.core                 CqlSession           ]
    [com.datastax.oss.driver.api.querybuilder         SchemaBuilder        ]
    [com.datastax.oss.driver.api.core.type            DataTypes            ]
    [com.datastax.oss.driver.api.core.metadata.schema ClusteringOrder      ]
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
          (.build))}
    )
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
  [session keyspace]
  (let [statement
        (->
          (SchemaBuilder/createTable (format "\"%s.table0\"" keyspace))
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

(defn clusterInfo
  [session]
  (log/info " :: Datacenter : Rack : Host+Port : State : # of connections")
  (doseq
    [m (getMetaData session)]
    (log/info (str "Node :: " m)))
  (doseq
    [k (getKeyspaces session)]
    (log/info (str "Keyspace :: " k))) )

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
          config          (:ok configMaybe)
          host            (get-in config [:cassandra-client :initial-server-host])
          port            (get-in config [:cassandra-client :initial-server-port])
          keyspace        (get-in config [:cassandra-client :keyspace])
          dc              (get-in config [:cassandra-client :dc])
          replication     (get-in config [:cassandra-client :replication-factor])
          durable-writes  (get-in config [:cassandra-client :durable-writes])
          _               (log/info "Connecting to cluster")
          sessionMaybe    (getSession host port dc)
        ]

        (if (:ok sessionMaybe)
          (let [session (:ok sessionMaybe)]

            (clusterInfo session)

            (log/info
              (getExecutedStatement
                (createKeyspace session keyspace {dc replication} durable-writes)))

            (log/info
              (getExecutedStatement
                (createTable0 session keyspace)))

            (exit 0))
          (do
            (log/error "Cassandra session could not be established")
            (log/error sessionMaybe)
            (exit 1)))
      )

     (catch Exception e
      (do
        (log/error (str "caught exception: " (.getMessage e)))
        (exit 1))))))
