(defproject hammer "0.1.0"
  :description "FIXME: write description"
  :url "https://127.0.0.1/"
  :license {
    :name "MIT"
    :url "https://opensource.org/licenses/MIT"
  }
  :dependencies [
    [org.clojure/clojure                          "1.10.1"  ]
    [com.datastax.cassandra/cassandra-driver-core "3.10.2"  ]
    [org.clojure/core.async                       "1.3.610" ]
    [org.clojure/tools.cli                        "1.0.194" ]
    [org.apache.logging.log4j/log4j-core          "2.13.3"  ]
    [org.apache.logging.log4j/log4j-api           "2.13.3"  ]
  ]
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
  :jvm-opts ["-Dclojure.tools.logging.factory=clojure.tools.logging.impl/slf4j-factory"]
  :repl-options
    {:init-ns hammer.main}
  :main
    hammer.main)
