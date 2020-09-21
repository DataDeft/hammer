(ns hammer.main
  (:require
    [clojure.edn            :as edn                 ]
    [clojure.core.async     :as async               ]
    [clojure.tools.cli      :refer [parse-opts]     ])
  ; Java
  (:import
    [java.io                          File                                  ]
    [java.util                        UUID                                  ]
    [clojure.lang                     PersistentHashMap PersistentArrayMap  ]
    [clojure.core.async.impl.channels ManyToManyChannel                     ]
    [com.datastax.driver.core         Cluster Cluster$Builder               ]
    [org.apache.logging.log4j         Logger LogManager                     ] )
  (:gen-class))

(def log (. LogManager getLogger "main"))

(defn read-file
  "Returns {:ok string } or {:error...}"
  [^String file]
  (try
    (cond
      (.isFile (File. file))
        {:ok (slurp file) }                         ; if .isFile is true {:ok string}
      :else
        (throw (Exception. "Input is not a file"))) ;the input is not a file, throw exception
  (catch Exception e
    {:error "Exception" :fn "read-file" :exception (.getMessage e) }))) ; catch all exceptions

(defn parse-edn-string
  "Returns the Clojure data structure representation of s"
  [s]
  (try
    {:ok (clojure.edn/read-string s)}
  (catch Exception e
    {:error "Exception" :fn "parse-config" :exception (.getMessage e)})))

(defn exit [n]
  (System/exit n))

(defn read-config
  "Returns the Clojure data structure version of the config file"
  [file]
  (let
    [ file-string (read-file file) ]
    (cond
      (contains? file-string :ok)
        ;this return the {:ok} or {:error} from parse-edn-string
        (parse-edn-string (file-string :ok))
      :else
        ;the read-file operation returned an error
        file-string)))

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

(def cli-options
  ;; An option with a required argument
  [ ["-c" "--config FILE" "Config file location" :default "conf/app.edn"]
    ["-h" "--help"] ])

(defn main
  [& args]
  (let
    [
      opts (parse-opts args cli-options) ]
    ; ^PersistentHashMap config (read-config "conf/app.edn") ]
    (. log info opts)))

