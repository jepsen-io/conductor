(defproject io.jepsen/conductor "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the Conductor workflow orchestrator."
  :url "https://github.com/jepsen/conductor"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.12-SNAPSHOT"
                  :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [io.jepsen/postgres "0.1.2-SNAPSHOT"]
                 [io.jepsen/redis "0.2.0-SNAPSHOT"]
                 [com.gfredericks/test.chuck "0.2.15"
                  :exclusions [org.clojure/test.check]]
                 ; The Conductor clients pull in slightly older versions of
                 ; jackson and slf4j
                 [org.conductoross/conductor-client "5.0.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]
                 [org.conductoross/java-sdk "5.0.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]

                 [io.orkes.conductor/orkes-conductor-client "5.0.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]]
  :repl-options {:init-ns jepsen.conductor.repl}
  :main jepsen.conductor.cli
  :aot [jepsen.conductor.cli]
  :jvm-opts ["-Djava.awt.headless=true"
             "-Djava.net.preferIPv4Stack=true"
             "-server"]
  :profiles {:uberjar {:aot :all}})
