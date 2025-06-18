(ns edd.test.fixture.execution
  (:require [edd.test.fixture.dal :as f]
            [edd.el.cmd :as cmd]
            [edd.el.event :as event]
            [edd.memory.event-store :as event-store]
            [clojure.tools.logging :as log]))

(defn fuuid [n]
  (let [tpl   "00000000-0000-0000-0000-000000000000"
        l-tpl (.length tpl)
        str-n (str n)
        l     (.length str-n)]
    (java.util.UUID/fromString
     (.concat (.substring tpl 0 (- l-tpl l))
              str-n))))

(defn process-cmd-response! [ctx cmd]
  (log/info "apply-cmd" cmd)
  (let [resp (f/handle-cmd (assoc ctx
                                  :no-summary true)
                           cmd)]
    (log/info "apply-cmd returned" resp)
    (doseq [id (distinct (map :id (:events resp)))]
      (event/handle-event (assoc ctx
                                 :apply {:aggregate-id id})))
    resp))

(defn process-next!
  [ctx]
  (if-let [cmd (event-store/peek-cmd!)]
    (process-cmd-response! ctx cmd)))

(defn process-all!
  [ctx]
  (loop []
    (when (process-next! ctx)
      (recur))))

(defn place-cmd!
  [& cmds]
  (doseq [cmd cmds]
    (if (:commands cmd)
      (event-store/enqueue-cmd! cmd)
      (event-store/enqueue-cmd! {:commands [cmd]}))))

(defn run-cmd!
  [ctx & cmd]
  (apply place-cmd! cmd)
  (process-all! ctx))
