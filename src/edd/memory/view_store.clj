(ns edd.memory.view-store
  (:require [clojure.data :refer [diff]]
            [clojure.tools.logging :as log]
            [lambda.test.fixture.state :refer [*dal-state*]]
            [edd.memory.search :refer [advanced-search-impl]]
            [edd.search :refer [with-init
                                simple-search
                                advanced-search
                                update-aggregate
                                get-by-id-and-version
                                get-snapshot]]
            [lambda.util :as util]))

(defn fix-keys
  [val]
  (-> val
      (util/to-json)
      (util/to-edn)))

(defn filter-aggregate
  [query aggregate]
  (let [res (diff aggregate query)]
    (and (= (second res) nil)
         (= (nth res 2) query))))

(defmethod simple-search
  :memory
  [{:keys [query]}]
  {:pre [query]}
  (into []
        (filter
         #(filter-aggregate
           (dissoc query :query-id)
           %)
         (->> @*dal-state*
              (:aggregate-store)))))

(defn update-aggregate-impl
  [{:keys [aggregate] :as ctx}]
  {:pre [aggregate]}
  (util/d-time
   (format "MemoryViewStore update-aggregate, id: %s, version: %s" (:id aggregate) (:version aggregate))
   (let [aggregate (fix-keys aggregate)]
     (swap! *dal-state*
            #(update % :aggregate-store
                     (fn [v]
                       (->> v
                            (filter
                             (fn [el]
                               (not= (:id el) (:id aggregate))))
                            (cons aggregate)
                            (sort-by (fn [{:keys [id]}] (str id)))))))))

  ctx)

(defmethod update-aggregate
  :memory
  [ctx]
  (update-aggregate-impl ctx))

(defmethod advanced-search
  :memory
  [ctx]
  (advanced-search-impl ctx))

(defmethod with-init
  :memory
  [ctx body-fn]
  (log/debug "Initializing memory view store")
  (if-not (:global @*dal-state*)
    (body-fn ctx)
    (binding [*dal-state* (atom {})]
      (body-fn ctx))))

(defn- aggregates-matching-id
  [id]
  (->> @*dal-state*
       (:aggregate-store)
       (filter #(= (:id %) id))))

(defmethod get-snapshot
  :memory
  [ctx id]
  (util/d-time
   (format "MemoryViewStore get-snapshot, id: %s" id)
   (->> (aggregates-matching-id id)
        (sort-by :version (fnil > 0 0))
        first)))

(defmethod get-by-id-and-version
  :memory
  [ctx id version]
  (util/d-time
   (format "MemoryViewStore get-by-id-and-version, id: %s, version: %s" id version)
   (->> (aggregates-matching-id id)
        (filter #(= (:version %) version))
        first)))

(defn register
  [ctx]
  (assoc ctx :view-store :memory))
