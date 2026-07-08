(defproject jepsen.conductor.worker "0.1.0-SNAPSHOT"
  :description "A small worker used in the Jepsen conductor test"
  :url "https://github.com/jepsen-io/conductor"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[clj-http "3.13.1"]
                 [io.jepsen/generator "0.1.2-SNAPSHOT"]
                 [org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.logging "1.3.1"]

                 ; The Conductor clients pull in slightly older versions of
                 ; jackson and slf4j
                 [org.conductoross/conductor-client "5.0.1"
                  :exclusions [;com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]
                 [org.conductoross/java-sdk "5.0.1"
                  :exclusions [;com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]

                 [io.orkes.conductor/orkes-conductor-client "5.0.1"
                  :exclusions [;com.fasterxml.jackson.core/jackson-core
                               org.slf4j/slf4j-api]]
                 [spootnik/unilog "0.7.32"]]
  :aot [jepsen.conductor.worker]
  :main jepsen.conductor.worker
  :repl-options {:init-ns jepsen.conductor.worker})
