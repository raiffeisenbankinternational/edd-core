(ns edd.el.ctx)

(defn put-aggregate
  [ctx aggregate]
  (assoc-in ctx [:edd-core :el :aggregate] aggregate))

(defn get-aggregate
  [ctx]
  (get-in ctx [:edd-core :el :aggregate]))

(defn get-effect-partition-size ^long
  [ctx]
  (get-in ctx [:edd-core :el :effect-partition-size] 10000))

(defn set-effect-partition-size
  [ctx size]
  (assoc-in ctx [:edd-core :el :effect-partition-size] size))
