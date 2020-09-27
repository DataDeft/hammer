(ns hammer.cass
  (:require
   [hammer.utils       :refer [exit rand-str2]]
   [clojure.core.async :as as]
   [clojure.tools.logging :as log])
  ; Java
  (:import
   [java.util                                    LinkedList]
   [com.basho.riak.client.api                    RiakClient]
   [com.basho.riak.client.core                   RiakNode$Builder 
                                                 RiakCluster$Builder 
                                                 RiakCluster]
   [com.basho.riak.client.core.query             Location Namespace]
   [com.basho.riak.client.api.commands.buckets   FetchBucketProperties 
                                                 StoreBucketProperties]))

(defn getBuilder
  ^RiakNode$Builder []
  (let [builder (->
                 (RiakNode$Builder.)
                 (.withMinConnections 16)
                 (.withMaxConnections 64)) ]
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
  [client namespace]
  (let [fetchProps (-> (FetchBucketProperties/Builder. namespace)
                       (.build))
        fetchResponse    (.execute ^RiakClient client fetchProps)
        bucketProperties (.getBucketProperties fetchResponse)      ]
    bucketProperties))

(defn setBucketProperties
  [_client _properties]
  0)
