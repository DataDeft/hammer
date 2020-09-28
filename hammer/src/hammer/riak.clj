(ns hammer.riak
  (:require
   [hammer.utils       :refer [exit rand-str2 deltaTimeMs]]
   [clojure.core.async :as as]
   [clojure.tools.logging :as log])
  ; Java
  (:import
   [java.util                                    UUID Random LinkedList]
   [com.basho.riak.client.api                    RiakClient]
   [com.basho.riak.client.core                   RiakNode$Builder 
                                                 RiakCluster$Builder 
                                                 RiakCluster]
   [com.basho.riak.client.core.query             Location Namespace]
   [com.basho.riak.client.api.commands.buckets   FetchBucketProperties$Builder 
                                                 StoreBucketProperties]
   [com.basho.riak.client.api.commands.kv        StoreValue$Builder]
))

(defn getBuilder
  ^RiakNode$Builder []
  (let [builder (->
                 (RiakNode$Builder.)
                 (.withMinConnections 64)
                 (.withMaxConnections 128)) ]
  builder))

(defn getAddresses 
  ^LinkedList [addrs]
  (let [addresses (LinkedList.)
        _ (doseq [a addrs] (.add addresses a))]
    addresses))

(defn getCluster
  ^RiakCluster [builder addresses]
  (let [nodes (RiakNode$Builder/buildNodes ^RiakNode$Builder builder ^LinkedList addresses)
        cluster (-> (RiakCluster$Builder. nodes) (.build))]
    cluster))

(defn connectToCluster
  ^RiakClient [cluster]
  (let [_ (.start cluster)
        client (RiakClient. cluster)]
    client))

(defn createNamespace 
  [name]
  (Namespace. name))

(defn getBucketProperties
  [client bucket]
  (let [fetchProps (-> (FetchBucketProperties$Builder. (createNamespace bucket))
                       (.build))
        fetchResponse    (.execute ^RiakClient client fetchProps)
        bucketProperties (.getBucketProperties fetchResponse)      ]
    bucketProperties))

(defn setBucketProperties
  [_client _properties]
  0)

(deftype Record
  [^String deviceId ^String hash ^bytes bob] 
  Object 
  (toString [_] (str "Record: " (format "%s : %s : %s" deviceId hash (String. bob)))))

(defn insertTask
  [riak-client riak-bucket runs iterations stat-chan hash-size]
  (dotimes [r runs]
    (try
      (let [^"[D" perf (make-array Double/TYPE iterations)
            namespace (Namespace. riak-bucket)]
        (log/info (format "Starting run: %s in thread: %s" r (.getName (Thread/currentThread))))
        ; starting run
        (dotimes [n iterations]
          (let [start           (System/nanoTime) ; nanoseconds
                random-string   (rand-str2 hash-size)
                record (Record. 
                        (.toString (UUID/randomUUID)) 
                        random-string
                        (.getBytes random-string))
                location (Location. namespace (.toString (UUID/randomUUID)))
                svb (->
                     (StoreValue$Builder. record)
                     (.withLocation location)
                     (.build))
                ]
            (.execute riak-client svb)
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
