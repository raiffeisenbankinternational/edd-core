(ns edd.search
  (:require [clojure.tools.logging :as log]))

(defn parse [ctx q]
  (let [% (first q)]
    (cond
      (vector? %) (recur ctx %)
      :else (apply (get ctx %) ctx (rest q)))))

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

(def default-size 50)
