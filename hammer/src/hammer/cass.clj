(ns hammer.cass
  (:require
   [hammer.utils       :refer [exit rand-str2 deltaTimeMs]]
   [clojure.core.async :as as]
   [clojure.tools.logging :as log])
  ; Java
  (:import
   [java.util                                        UUID Random]
   [java.io                                          File]
   [java.net                                         InetSocketAddress]
   [java.nio                                         ByteBuffer]
   [com.datastax.oss.driver.api.core                 CqlSession CqlIdentifier
                                                     DefaultConsistencyLevel 
                                                     DriverTimeoutException]
   [com.datastax.oss.driver.api.core.config          DriverConfigLoader]
   [com.datastax.oss.driver.api.querybuilder         SchemaBuilder QueryBuilder
                                                     SchemaBuilder$RowsPerPartition]
   [com.datastax.oss.driver.internal.core.session    DefaultSession]
   [com.datastax.oss.driver.internal.core.cql        DefaultPreparedStatement]
   [com.datastax.oss.driver.api.core.type            DataTypes]
   [com.datastax.oss.driver.api.core.metadata.schema ClusteringOrder]) )

(def fs (System/getProperty "file.separator"))

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
  [host port datacenter keyspace applicationConfig]
  (try
    (let [contactPoint (InetSocketAddress. host port)
          configFile (File. applicationConfig)]
      {:ok
       (->
        (CqlSession/builder)
        (.addContactPoint contactPoint)
        (.withLocalDatacenter datacenter)
        (.withConfigLoader (DriverConfigLoader/fromFile configFile))
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
  (let [statement
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
  (try
    (let [statement
          (->
           (SchemaBuilder/createTable "table0")
           (.ifNotExists)
           (.withPartitionKey            "userId"    DataTypes/UUID)
           (.withClusteringColumn        "deviceId"  DataTypes/UUID)
           (.withColumn                  "hash"      DataTypes/TEXT)
           (.withColumn                  "bob"       DataTypes/BLOB)
           (.withClusteringOrder         "deviceId"  ClusteringOrder/ASC)
           (.withLZ4Compression          4 0.8)
           (.withMemtableFlushPeriodInMs 1024)
           (.withCaching true (SchemaBuilder$RowsPerPartition/ALL))
           (.build))]
      (log/info
       (getExecutedStatement (.execute session statement))))
    (catch Exception e
      (log/info (format "Exception %s" (.getMessage e)))
      (exit 1))))

(defn logClusterInfo
  [session]
  (log/info " :: Datacenter : Rack : Host+Port : State : # of connections")
  (doseq
   [m (getMetaData session)]
    (log/info (str "Node :: " m)))
  (doseq
   [k (getKeyspaces session)]
    (log/info (str "Keyspace :: " k))))

(defn createKeyspaceWithInitialSession
  [host port dc keyspace replication durable-writes]
  (let [initialSessionMaybe (getSession host port dc)]
    (if (:ok initialSessionMaybe)
        ; ok
      (let [initial-session (:ok initialSessionMaybe)]
        (logClusterInfo initial-session)
        (log/info
         (getExecutedStatement
          (createKeyspace initial-session keyspace {dc replication} durable-writes)))
        (log/info "Closing initial session")
        (.close initial-session))
        ; err
      (do
        (log/error "Initial Cassandra session could not be established")
        (log/error initialSessionMaybe)
        (exit 1)))))

(defn insertIntoTable0NoBind
  [session userid deviceid hash bob]
  (let [statement
        (->
         (QueryBuilder/insertInto "table0")
         (.value "userid"    (QueryBuilder/literal userid))
         (.value "deviceid"  (QueryBuilder/literal deviceid))
         (.value "hash"      (QueryBuilder/literal hash))
         (.value "bob"       bob)
         (.build))]
    (.execute ^DefaultSession session statement)))

(defn writeToLocalDisk
  [s]
  (try
    (.createNewFile (File. (str "uids" fs s)))
    (catch Exception e
      (log/info (format "Exception %s" (.getMessage e))))))

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
  (try
    (writeToLocalDisk userid)
    (let [statement (getInsertStatementBind)
          prepared  (.prepare ^DefaultSession session statement)
          bound     (.bind ^DefaultPreparedStatement prepared (into-array Object [userid deviceid hash (ByteBuffer/wrap bob)]))]
      (.execute ^DefaultSession session bound))
    (catch DriverTimeoutException e
      (log/info (format "DriverTimeoutException %s" (.getMessage e))))
    (catch Exception e
      (log/info (format "Unknown Exception %s" (.getMessage e)))
      (exit 1))))

(defn insertTaskOneSession
  [session runs iterations stat-chan hashSize]
  (dotimes [r runs]
    (try
      (let [^"[D" perf (make-array Double/TYPE iterations)]
        (log/info (format "Starting run: %s in thread: %s" r (.getName (Thread/currentThread))))
        ; starting run
        (dotimes [n iterations]
          (let [start     (System/nanoTime) ; nanoseconds
                userId    (UUID/randomUUID)
                deviceId  (UUID/randomUUID)
                hash      (rand-str2 hashSize)
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

(defn getSelectStatement
  []
  (->
   (QueryBuilder/selectFrom "table0")
   (.column "bob")
   (.whereColumn "userid")
   (.isEqualTo (QueryBuilder/bindMarker))
   (.build)))

(defn selectFromTable0
  [session userid]
  (try
    (let [statement (getSelectStatement)
          prepared  (.prepare ^DefaultSession session statement)
          bound     (.bind ^DefaultPreparedStatement prepared (into-array Object [(UUID/fromString userid)]))]
      (.execute ^DefaultSession session bound))
    (catch DriverTimeoutException e
      (log/info (format "DriverTimeoutException %s" (.getMessage e))))
    (catch Exception e
      (log/info (format "Unknown Exception %s" (.getMessage e))))))

(defn selectTaskOneSession
  [session runs iterations stat-chan files]
  (let [rand (Random.)
        len (count files)]
    (dotimes [r runs]
      (try
        (let [^"[D" perf (make-array Double/TYPE iterations)]
          (log/info (format "Starting run: %s in thread: %s" r (.getName (Thread/currentThread))))
          (dotimes [n iterations]
            (let [start     (System/nanoTime)]
              (selectFromTable0 session (aget files (.nextInt ^Random rand len)))
              (aset perf n (deltaTimeMs start (System/nanoTime)))))
          (log/info (format "Finished run: %s in thread: %s" r (.getName (Thread/currentThread))))
          (as/>!!
           stat-chan
           {:thread-name (.getName (Thread/currentThread)) :run r :perf perf}))
        {:ok :ok}
        (catch Exception e
          (log/error (str "caught exception: " (.getMessage e)))
          (log/error e)
          {:err :err})))))
