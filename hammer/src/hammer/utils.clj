(ns hammer.utils
  (:require
    [clojure.edn            :as edn                 ]
    [clojure.tools.cli      :refer [parse-opts]     ]
    [clojure.reflect        :refer [reflect]        ]
    )
  ; Java
  (:import
    [java.io               File        ]
    [java.util             UUID Random ]
    )

  )

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

(def cli-options
  ;; An option with a required argument
  [ ["-c" "--config FILE" "Config file location" :default "conf/app.edn"]
    ["-h" "--help"] ])

(defn getOpts
  [args cli-options]
  (parse-opts args cli-options))

(defn all-methods [x]
    (->> x reflect
           :members
           (filter :return-type)
           (map :name)
           sort
           (map #(str "." %) )
           distinct
           println))

(defn uuid
  "Returns a new java.util.UUID as string"
  []
  (.toString (UUID/randomUUID)))

(defn rand-str
  [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn rand-str2-slow
  [len]
  (let [leftLimit 97
        rightLimit 122
        random (Random.)
        stringBuilder (StringBuilder. len)
        diff (- rightLimit leftLimit)]
    (dotimes [_ len]
      (let [ch (char (.intValue (+ leftLimit (* (.nextFloat random) (+ diff 1)))))]
        (.append stringBuilder ch)))
    (.toString stringBuilder)))

(defn rand-str2
  ^String [^Long len]
  (let [leftLimit 97
        rightLimit 122
        random (Random.)
        stringBuilder (StringBuilder. len)
        diff (- rightLimit leftLimit)]
    (dotimes [_ len]
      (let [ch (char (.intValue ^Double (+ leftLimit (* (.nextFloat ^Random random) (+ diff 1)))))]
        (.append ^StringBuilder stringBuilder ch)))
        (.toString ^StringBuilder stringBuilder)))
