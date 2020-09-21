(ns hammer.core
  (:import
    (com.datastax.driver.core
      Cluster
      Cluster$Builder)))

(defn cluster
  [serverAddress]
  (.build (.addContactPoint (Cluster$Builder.) serverAddress)))

(defn session
  [cluster db]
  (.connect cluster db))

(defn getConnectedHosts
  [session]
  (for [host (.getConnectedHosts (.getState session))]
    (str
      (.getDatacenter host)
      " :: " (.getRack host)
      " :: " (.getHostAddress (.getAddress host))
      " :: " (.getState host)
      " :: " (.toString (.getCassandraVersion host)))))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
