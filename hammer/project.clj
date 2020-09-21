(defproject hammer "0.1.0"
  :description "FIXME: write description"
  :url "https://127.0.0.1/"
  :license {
    :name "MIT"
    :url "https://opensource.org/licenses/MIT"
  }
  :dependencies [
    [org.clojure/clojure                          "1.10.1"]
    [com.datastax.cassandra/cassandra-driver-core "3.10.2"]
  ]
  :repl-options {:init-ns hammer.core})
