(ns edd.el.ctx)

(defn put-aggregate
  [ctx aggregate]
  (assoc-in ctx [:edd-core :el :aggregate] aggregate))

(defn get-aggregate
  [ctx]
  (get-in ctx [:edd-core :el :aggregate]))
