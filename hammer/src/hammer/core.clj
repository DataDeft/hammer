(ns hammer.core
  (:require
   [hammer.utils       :refer [exit read-config getOpts]]
   [hammer.cass           :as cass]
   [clojure.core.async    :as as   ]
   [clojure.tools.logging :as log  ])
  ; Java
  (:import
   [java.util                                        Arrays]
   [java.io                                          File])
  (:gen-class))

(System/setProperty "clojure.core.async.pool-size" "2")

(log/info (format "clojure.core.async.pool-size %s" (System/getProperty "clojure.core.async.pool-size")))

(defn -main
  [& args]
  (try
    (let [options                 (getOpts args)
          _                       (log/info options)
          config-file             (get-in options [:options :config])
          test-mode               (get-in options [:options :mode])
          _                       (log/info (format "Test mode: %s" test-mode))
          database                (get-in options [:options :database])
          _                       (log/info (format "Database flavor: %s" database))
          config                  (:ok (read-config config-file))
          _                       (if (nil? config) (exit 1) (log/info config))
          host                    (get-in config [:cassandra-client :initial-server-host])
          port                    (get-in config [:cassandra-client :initial-server-port])
          keyspace                (get-in config [:cassandra-client :keyspace])
          dc                      (get-in config [:cassandra-client :dc])
          replication             (get-in config [:cassandra-client :replication-factor])
          durable-writes          (get-in config [:cassandra-client :durable-writes])
          iterations              (get-in config [:hammer :number-of-iterations])
          runs                    (get-in config [:hammer :number-of-runs])
          channel-timeout         (get-in config [:hammer :channel-timeout])
          thread-count            (get-in config [:hammer :thread-count])
          application-config-path (get-in config [:cassandra-client :application-config-path])
          hash-size               (get-in config [:cassandra-tables :table0 :hash-size])
          _                   (log/info "Connecting to cluster")
          initialSessionMaybe (cass/getSession host port dc)]
      ; creating keyspace
      (if (:ok initialSessionMaybe)
        ; ok
        (let [initial-session (:ok initialSessionMaybe)]
          (cass/logClusterInfo initial-session)
          (log/info
           (cass/getExecutedStatement
            (cass/createKeyspace initial-session keyspace {dc replication} durable-writes)))
          (log/info "Closing initial session")
          (.close initial-session))
        ; err
        (do
          (log/error "Initial Cassandra session could not be established")
          (log/error initialSessionMaybe)
          (exit 1)))
      ; creating threads and running inserts
      (let
        [ stat-chan (as/chan 8)
         runResults (atom {})
         testSession (:ok (cass/getSessionWithKeyspace host port dc keyspace application-config-path)) ]

        (cond
          (= database "cassandra")
            (if (= test-mode "write")
              (do
                (log/info "Test mode: Write")
                (cass/createTable0 testSession)
                (dotimes [_ thread-count]
                  (as/thread
                    (Thread/sleep 100)
                    (cass/insertTaskOneSession testSession runs iterations stat-chan hash-size))))
            ; else
              (do
                (log/info "Test mode: Read")
                (let [files (.list (File. "uids"))]
                  (log/info (format "Number of files %s" (count files)))
                  (dotimes [_ thread-count]
                    (as/thread
                      (Thread/sleep 100)
                      (cass/selectTaskOneSession testSession runs iterations stat-chan files))))))
          (= database "riak")
            (exit 0)
          :else
          (do
            (log/info "Nor Cassandra or Riak??")
            (exit 0)))

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
                        (swap! runResults assoc-in [msg-run] {:totalTime totalTime :performance performance :percentiles {:p50 p50 :p90 p90 :p99 p99 :999 p999 :p100 p100}})
                        (log/info (get-in @runResults [msg-run])))
                      ; else
                      (log/debug "Got message from a thread but the measurement is not complete yet")))
                  ;else - timeout
                  (do
                    (log/info "Channel timed out. Stopping...")
                    (doseq [r @runResults] (log/info r))
                    (exit 0)))))))))
    (catch Exception e
      (log/error (str "caught exception: " (.getMessage e)))
      (log/error e)
      (exit 1))))
