(ns jepsen.conductor.nemesis
  "Fault injection."
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap loopr]]
            [jepsen [control :as c]
                    [db :as jdb]
                    [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]
                    [random :as rand]
                    [role :as role]]
            [clojure.set :as set]
            [jepsen.control.util :as cu]
            [jepsen.nemesis [combined :as nc]
                            [membership :as m]
                            [file :as nf]
                            [time :as nt]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn package-gen-helper
  "Helper for package-gen. Takes a collection of packages and draws a random
  nonempty subset of them."
  [packages]
  (when (seq packages)
    (let [pkgs (->> packages
                    ; And pick a random subset of those
                    util/random-nonempty-subset
                    vec)]
      ; If we drew nothing, try again.
      (if (seq pkgs)
        pkgs
        (do ; (info "no draw, retrying")
            (recur packages))))))

(defn package-gen
  "For long-running tests, it's nice to be able to have periods of no faults,
  periods with lots of faults, just one kind of fault, etc. This takes a time
  period in seconds, which is how long to emit nemesis operations for a
  particular subset of packages. Takes a collection of packages. Constructs a
  nemesis generator which emits faults for a shifting collection of packages
  over time."
  [period packages]
  ; We want a sequence of random subsets of packages
  (repeatedly
    (fn rand-pkgs []
      (let [; Pick packages
            pkgs (if (< (rand) 1/4)
                   ; Roughly 1/4 of the time, pick no pkgs
                    []
                    (package-gen-helper packages))
            ; Construct combined generators
            gen       (if (seq pkgs)
                        (apply gen/any (map :generator pkgs))
                        (gen/sleep period))
            final-gen (keep :final-generator pkgs)]
        ; Ops from the combined generator, followed by a final gen
        [(gen/log (str "Shifting to new mix of nemeses: "
                       (pr-str (map (comp n/fs :nemesis) pkgs))))
         (gen/time-limit period gen)
         final-gen]))))

;; Role-specific packages

(defn role-faults
  "Takes a set of faults like #{:pause-db}, a role name, and the nodes involved
  in that role. Returns a set of faults for a specific role like #{:pause}.

  Some faults, like :partition, make no sense applied to single-node roles;
  they are omitted.

  General faults, like :pause or :kill, apply to every role."
  [faults role nodes]
  (->> faults
       (keep (fn [fault]
               (if-let [[m fault role']
                        (re-find #"^(\w+)-(postgres|redis|server|std-worker|worker)$"
                                 (name fault))]
                 ; This is a role-specific fault
                 (when (= (name role) role')
                   (keyword fault))
                 ; A general fault
                 (case role
                   ; I don't think the workers ever talk to each other
                   ; directly, so we don't bother partitioning them. PG is a
                   ; single node, so same there.
                   (:std-worker :worker :postgres)
                   ((complement #{:partition}) fault)

                   ; Otherwise you get to do everything.
                   fault))))
       set))

(defn update-role-faults
  "Takes CLI options and updates :faults to be specific to the given role."
  [role opts]
  (update opts :faults role-faults role (get (:roles opts) role)))

(defn package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        roles (:roles opts)
        _ (assert (map? roles))
        packages
        (->> (concat
               ; Unfurl each role into a collection of nemesis packages for
               ; that particular role.
               (mapcat (fn [[role nodes]]
                         (let [opts (if (= 1 (count nodes))
                                      ; For single-node roles, we don't bother
                                      ; with partitions, and we also use :all
                                      ; for our pause and kill targets; no
                                      ; sense in the other options.
                                      (-> opts
                                          (assoc-in [:pause :targets] [:all])
                                          (assoc-in [:kill :targets] [:all]))
                                      opts)]
                           (->> opts
                                (update-role-faults role)
                                nc/nemesis-packages
                                (map (partial role/restrict-nemesis-package role)))))
                       roles)
               ; Custom packages
               [])
             (filter :generator))
        nsp (:stable-period opts)]
    ;(info :packages (map (comp n/fs :nemesis) packages))
    (cond-> (nc/compose-packages packages)
      ; Right now I'd actually like to run the full set of faults
      ;nsp (assoc :generator (package-gen nsp packages))
      )))
