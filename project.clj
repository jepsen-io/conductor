(defproject io.jepsen/conductor "0.1.1-SNAPSHOT"
  :description "Jepsen tests for the Conductor workflow orchestrator."
  :url "https://github.com/jepsen/conductor"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[clj-http "3.13.1"
                  :exclusions [commons-io
                               commons-codec
                               potemkin]]
                 [com.gfredericks/test.chuck "0.2.15"
                  :exclusions [org.clojure/test.check]]
                 [io.jepsen/postgres "0.1.2-SNAPSHOT"]
                 [io.jepsen/redis "0.2.0-SNAPSHOT"]
                 ; The Conductor clients pull in slightly older versions of
                 ; jackson and slf4j
                 [jepsen "0.3.12"
                  :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [org.conductoross/conductor-client "5.0.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]
                 [org.conductoross/java-sdk "5.0.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]
                 [org.clojure/clojure "1.12.5"]

                 [io.orkes.conductor/orkes-conductor-client "5.0.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]]
  :repl-options {:init-ns jepsen.conductor.repl}
  :main jepsen.conductor.cli
  :aot [jepsen.conductor.cli]
  :jvm-opts ["-Djava.awt.headless=true"
             "-Djava.net.preferIPv4Stack=true"
             ;"-XX:-OmitStackTraceInFastThrow"
             "-server"]
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[criterium "0.4.6"]]}}
  :test-selectors {:focus :focus})
