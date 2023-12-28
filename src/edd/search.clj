(ns edd.search
  (:require [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn parse
; TODO parse => build-filter
  [op->filter-builder filter-spec]
  (let [[fst & rst] filter-spec]
    (if (vector? fst)
      (recur op->filter-builder fst)
      (let [builder-fn (get op->filter-builder fst)]
        (apply builder-fn op->filter-builder rst)))))

(defmulti update-aggregate
  (fn [ctx] (:view-store ctx)))

(defmulti advanced-search
  (fn [ctx] (:view-store ctx)))

(defmulti simple-search
  (fn [{:keys [query] :as ctx}]
    (:view-store ctx)))

(defmulti update-aggregate
  (fn [{:keys [aggregate] :as ctx}]
    (:view-store ctx)))

(defmulti with-init
  (fn [ctx body-fn]
    (:view-store ctx)))

(defmethod with-init
  :default
  [ctx body-fn]
  (log/info "Default search init")
  (body-fn ctx))

(defmulti get-snapshot
  (fn [ctx id]
    (:view-store ctx :default)))

(defmethod get-snapshot
  :default
  [ctx id]
  nil)

(def default-size 50)
