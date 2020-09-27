(defproject hammer "0.1.0"
  :description "FIXME: write description"
  :url "https://127.0.0.1/"
  :license {
    :name "MIT"
    :url "https://opensource.org/licenses/MIT"
  }
  :dependencies [[org.clojure/clojure                          "1.10.1"]
                 [com.datastax.oss/java-driver-core            "4.9.0"]
                 [com.datastax.oss/java-driver-query-builder   "4.9.0"]
                 [org.clojure/core.async                       "1.3.610"]
                 [org.clojure/tools.cli                        "1.0.194"]
                 [org.clojure/tools.logging                    "1.1.0"]
                 [ch.qos.logback/logback-classic               "1.2.3"]
                 [org.lz4/lz4-java                             "1.7.1"]
                 [criterium                                    "0.4.6"]
                 [org.clojure/test.check                       "1.1.0"]
                 [com.basho.riak/riak-client                   "2.1.1"]]
  :exclusions [
    javax.mail/mail
    javax.jms/jms
    com.sun.jdmk/jmxtools
    com.sun.jmx/jmxri
    jline/jline
  ]
  :profiles {
    :uberjar {
      :aot :all
    }
  }
  :repl-options
    {:init-ns hammer.core}
  :main
    hammer.core)
