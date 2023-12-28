(ns edd.flow)

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defmacro e->
  "When expr does not contain :error key, threads it into the first form (via ->),
  and when that result does not contain :error key, through the next etc"
  {:added "1.5"}
  [expr & forms]
  (let [g (gensym)
        steps (map (fn [step] `(if (:error ~g) ~g (-> ~g ~step)))
                   forms)]
    `(let [~g ~expr
           ~@(interleave (repeat g) (butlast steps))]
       ~(if (empty? steps)
          g
          (last steps)))))
