(ns jepsen.conductor.core
  "Unlike most Jepsen tests, we want our history to include not only operations
  we asked for, but also operations recorded from the workers. Workers send
  their operations to a small web server, which passes them to the history
  writer. We modify the standard Jepsen interpreter and storage system so that
  operation indices are assigned by the history writer, rather than the
  generator. This allows our web server to interleave its own events into the
  history (at the cost of the generator no longer knowing operation indices,
  which is, I believe, OK for our purposes).

  Before beginning work, the worker sends the test harness a message like:

      {:type :invoke, :process \"n3-2\", :f :execute, :value ...}

  And when just before the worker completes the work, it sends another:

      {:type :ok, :process \"n3-2\", :f :execute, :value ...}

  Since each of these messages are synchronous, we know that the worker must
  have received a call to `worker.execute()` some time before the invoke, and
  returned from `execute()` some time after the ok. This gives us a lower
  bound: we know the worker's execute() method was running for *at least* that
  interval.

  All of this is an *incredible* pile of hacks and state being thrown all over
  the place--this is one of the few times that Jepsen's design really wasn't
  cleanly extensible to a problem. If you're here in the future and trying to
  debug this, my apologies."
  (:refer-clojure :exclude [run!])
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [datafy :refer [datafy]]
                     [edn :as edn]
                     [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [client     :as client]
                    [core       :as jepsen]
                    [print      :refer [log-op]]
                    [generator  :as gen]
                    [history    :as h]
                    [nemesis    :as nemesis]
                    [util       :as util :refer [exception-message]]]
            [jepsen.generator [interpreter :as interpreter]
                              [context :as context]]
            [jepsen.store.format :as store.format]
            [org.httpkit.server :as http-server])
  (:import (java.io Closeable
                    InputStream
                    PushbackReader)
           (java.util.concurrent ArrayBlockingQueue
                                 BlockingQueue
                                 TimeUnit)
           (java.util.concurrent.atomic AtomicLong)
           (io.lacuna.bifurcan Set)))

(def history-port
  "The port for our history-logging HTTP server"
  8888)

(defn validate-op
  "Throws if we get an illegal op structure; otherwise returns it with index
  -1."
  [op]
  (assert (map? op))
  (assert (set/subset? (set (keys op)) #{:process :type :f :value :error}))
  (assert (keyword? (:process op)))
  (assert (#{:invoke :ok :info :fail} (:type op)))
  (assert (keyword? (:f op)))
  (assoc op :index -1))

(defn http-server
  "Spawns an HTTP server which accepts EDN operations and journals them to the
  history. Takes a promise of a history writer."
  [history-writer]
  (info "Starting history HTTP history on port" history-port)
  (http-server/run-server
    ; We need bound-fn to pick up on the Jepsen relative time origin
    (bound-fn handle [req]
      (let [body  (:body req)
            _     (assert (instance? InputStream body))
            op    (-> body
                      io/reader
                      (PushbackReader.)
                      edn/read
                      validate-op
                      h/op
                      (assoc :time (util/relative-time-nanos)))]
        (info "HTTP op" (pr-str op))
        (log-op op)
        (store.format/append-to-big-vector-block!
          history-writer op)))
    {:port history-port}))

(defn big-vector-block-writer-worker!
  "Loop which writes values from a BigVectorBlockWriter's queue to disk."
  [handle block-id elements-per-chunk ^BlockingQueue queue]
  (info "Using Conductor big-vector-block-writer-worker!")
  (try
    (loop [; Next op index
           index 0
           ; Number of elements written to completed blocks. We use
           ; this for the initial block index of each streaming
           ; block.
           completed-count 0
           ; How many elements have we written to the current,
           ; uncompleted chunk?
           uncompleted-count 0
           ; Vector of [initial-index block-id] chunks
           chunks []
           ; Streaming block writer for current chunk
           ^Closeable chunk-writer
           (store.format/fressian-stream-block-writer! handle)]
      ; Write elements
      (let [element (.take queue)]
        (if (identical? :jepsen.store.format/finished element)
          ; Wrap up!
          (do (.close chunk-writer)
              (let [chunks (conj chunks [completed-count
                                         (:block-id chunk-writer)])]
                (store.format/write-big-vector-block! handle
                                         block-id
                                         (+ completed-count uncompleted-count)
                                         chunks)
                (store.format/write-block-index! handle)))
          ; Write element
          (if (< uncompleted-count elements-per-chunk)
            ; Room in this chunk
            (do (store.format/append-to-fressian-stream-block!
                  chunk-writer (assoc element :index index))
                (recur (inc index)
                       completed-count
                       (inc uncompleted-count)
                       chunks
                       chunk-writer))
            ; Chunk full; seal and start a fresh one
            (do (.close chunk-writer)
                (let [chunks (conj chunks [completed-count
                                           (:block-id chunk-writer)])
                      completed-count (+ completed-count uncompleted-count)]
                  (store.format/write-big-vector-block! handle
                                                        block-id
                                                        completed-count
                                                        chunks)
                  (store.format/write-block-index! handle)
                  (let [writer' (store.format/fressian-stream-block-writer!
                                  handle)]
                    (store.format/append-to-fressian-stream-block!
                      writer' (assoc element :index index))
                    (recur (inc index) completed-count 1 chunks writer'))))))))
    (catch Throwable t
      (warn t "Big vector block writer crashed!")
      (throw t))))

(defn run-interpreter!
  "Our version of jepsen.generator.interpreter/run! Identical, except it leaves
  indexes out of operations, and also spawns a web server."
  [test]
  (info "Using Conductor test interpreter")
  (gen/init!)
  (let [history-writer (:history-writer (:store test))
        http-server (http-server history-writer)
        _           (assert history-writer)
        ctx         (gen/context test)
        worker-ids  (gen/all-threads ctx)
        completions (ArrayBlockingQueue.
                      (.size ^io.lacuna.bifurcan.ISet worker-ids))
        workers     (mapv (partial interpreter/spawn-worker test completions
                                   (interpreter/client-nemesis-worker))
                          worker-ids)
        invocations (into {} (map (juxt :id :in) workers))
        gen         (->> (:generator test)
                         deref
                         gen/friendly-exceptions
                         gen/validate)
        ; Forget generator
        _           (util/forget! (:generator test))
        test        (dissoc test :generator)]
    (try+
      (loop [ctx            ctx
             gen            gen
             outstanding    0     ; Number of in-flight ops
             ; How long to poll on the completion queue, in micros.
             poll-timeout   0]
        ; First, can we complete an operation? We want to get to these first
        ; because they're latency sensitive--if we wait, we introduce false
        ; concurrency.
        (if-let [op' (.poll completions poll-timeout TimeUnit/MICROSECONDS)]
          (let [;_      (prn :completed op')
                thread (gen/process->thread ctx (:process op'))
                time    (util/relative-time-nanos)
                ; Update op with index and new timestamp
                goes-in-history? (interpreter/goes-in-history? op')
                op'     (assoc op' :time time)
                ; Update context with new time and thread being free
                ctx     (context/free-thread ctx time thread)
                ; Let generator know about our completion. We use the context
                ; with the new time and thread free, but *don't* assign a new
                ; process here, so that thread->process recovers the right
                ; value for this event.
                gen     (gen/update gen test ctx op')
                ; Threads that crash (other than the nemesis), or which
                ; explicitly request a new process, should be assigned new
                ; process identifiers.
                ctx     (if (and (not= :nemesis thread)
                                 (or (= :info (:type op'))
                                     (:end-process? op')))
                          (context/with-next-process ctx thread)
                          ctx)]
            ; Log completion and move on
            (if goes-in-history?
              (do (store.format/append-to-big-vector-block!
                    history-writer op')
                  (recur ctx gen (dec outstanding) 0))
              (recur ctx gen (dec outstanding) 0)))

          ; There's nothing to complete; let's see what the generator's up to
          (let [time        (util/relative-time-nanos)
                ctx         (assoc ctx :time time)
                ;_ (prn :asking-for-op)
                ;_ (binding [*print-length* 12] (pprint gen))
                [op gen']   (gen/op gen test ctx)]
            ;_ (prn :time time :got op)]
            (condp = op
              ; We're exhausted, but workers might still be going.
              nil (if (pos? outstanding)
                    ; Still waiting on workers
                    (recur ctx gen outstanding
                           (long interpreter/max-pending-interval))
                    ; Good, we're done. Tell workers to exit...
                    (do (doseq [[thread queue] invocations]
                          (.put ^ArrayBlockingQueue queue {:type :exit}))
                        ; Wait for exit
                        (dorun (map (comp deref :future) workers))
                        ; And return test
                        test))

              ; Nothing we can do right now. Let's try to complete something.
              :pending (recur ctx gen outstanding (long interpreter/max-pending-interval))

              ; Good, we've got an invocation.
              (if (< time (:time op))
                ; Can't evaluate this op yet!
                (do ;(prn :waiting (util/nanos->secs (- (:time op) time)) "s")
                    (recur ctx gen outstanding
                           ; Unless something changes, we don't need to ask
                           ; the generator for another op until it's time.
                           (long (/ (- (:time op) time) 1000))))

                ; Good, we can run this.
                (let [thread (gen/process->thread ctx (:process op))
                      goes-in-history? (interpreter/goes-in-history? op)
                      ; Log the invocation
                      _ (when goes-in-history?
                          (store.format/append-to-big-vector-block!
                            history-writer op))
                      ; Dispatch it to a worker
                      _ (.put ^ArrayBlockingQueue (get invocations thread) op)
                      ; Update our context to reflect
                      ctx (context/busy-thread ctx
                                               (:time op) ; Use time instead?
                                               thread)
                      ; Let the generator know about the invocation
                      gen' (gen/update gen' test ctx op)]
                  (recur ctx gen' (inc outstanding) 0)))))))

      (catch Throwable t
        ; We've thrown, but we still need to ensure the workers exit.
        (info "Shutting down workers after abnormal exit")
        ; We only try to cancel each worker *once*--if we try to cancel
        ; multiple times, we might interrupt a worker while it's in the
        ; finally block, cleaning up its client.
        (dorun (map (comp future-cancel :future) workers))
        ; If for some reason *that* doesn't work, we ask them all to exit via
        ; their queue.
        (loop [unfinished workers]
          (when (seq unfinished)
            (let [{:keys [in future] :as worker} (first unfinished)]
              (if (future-done? future)
                (recur (next unfinished))
                (do (.offer ^java.util.Queue in {:type :exit})
                    (recur unfinished))))))
        (throw t))
      (finally
        (http-server)))))

(let [core-run! jepsen/run!]
  (defn run-test!
    "Our version of jepsen.core/run!"
    [test]
    (info "Using Conductor jepsen.core/run!")
    (with-redefs [jepsen.store.format/big-vector-block-writer-worker!
                  big-vector-block-writer-worker!

                  jepsen.generator.interpreter/run! run-interpreter!]
      (core-run! test))))
