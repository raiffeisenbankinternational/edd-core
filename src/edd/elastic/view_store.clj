(ns edd.elastic.view-store
  (:gen-class)
  (:require [clojure.test :refer :all]
            [edd.search :refer [with-init
                                parse
                                simple-search
                                default-size
                                advanced-search
                                update-aggregate
                                get-snapshot
                                get-by-id-and-version]]
            [edd.postgres.history :as history]
            [edd.postgres.common :refer [->realm ->service]]
            [lambda.util :as util]
            [lambda.elastic :as elastic]
            [edd.s3.view-store :as s3.vs]
            [clojure.tools.logging :as log]
            [clojure.string :as string]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn realm
  [ctx]
  (name (get-in ctx [:meta :realm] :no_realm)))

(defn- make-index-name
  [realm-name service-name]
  (str realm-name "_" (string/replace (name service-name) "-" "_")))

(defn- field+keyword
  [field]
  (str (name field) ".keyword"))

(defn ->trim [v]
  (if (string? v)
    (string/trim v)
    v))

(def op->filter-builder
  {:and      (fn [op->fn & filter-spec]
               {:bool
                {:filter (mapv #(parse op->fn %) filter-spec)}})
   :or       (fn [op->fn & filter-spec]
               {:bool
                {:should               (mapv #(parse op->fn %) filter-spec)
                 :minimum_should_match 1}})
   :eq       (fn [_ & [a b]]
               {:term
                {(field+keyword a) (->trim b)}})
   :=        (fn [_ & [a b]]
               {:term
                {(name a) (->trim b)}})
   :wildcard (fn [_ & [a b]]
               {:bool
                {:should
                 [{:wildcard
                   {(str (name a)) {:value (str "*" (->trim b) "*")}}}
                  {:match_phrase
                   {(str (name a)) (str (->trim b))}}]}})
   :not      (fn [op->fn & filter-spec]
               {:bool
                {:must_not (parse op->fn filter-spec)}})
   :in       (fn [_ & [a b]]
               {:terms
                {(field+keyword a) b}})
   :exists   (fn [_ & [a _]]
               {:exists
                {:field (name a)}})
   :lte      (fn [_ & [a b]]
               {:range
                {(name a) {:lte b}}})
   :gte      (fn [_ & [a b]]
               {:range
                {(name a) {:gte b}}})
   :nested   (fn [op->fn path & filter-spec]
               {:bool
                {:must [{:nested {:path  (name path)
                                  :query (mapv #(parse op->fn %) filter-spec)}}]}})})

(defn search-with-filter
  [filter q]
  (let [[fields-key fields value-key value] (:search q)
        search (mapv
                (fn [p]
                  {:bool
                   {:should               [{:match
                                            {(str (name p))
                                             {:query value
                                              :boost 2}}}
                                           {:wildcard
                                            {(str (name p)) {:value (str "*" (->trim value) "*")}}}]
                    :minimum_should_match 1}})
                fields)]

    (-> filter
        (assoc-in [:query :bool :should] search)
        (assoc-in [:query :bool :minimum_should_match] 1))))

(defn form-sorting
  [sort]
  (map
   (fn [[a b]]
     (case (keyword b)
       :asc {(field+keyword a) {:order "asc"}}
       :desc {(field+keyword a) {:order "desc"}}
       :asc-number {(str (name a) ".number") {:order "asc"}}
       :desc-number {(str (name a) ".number") {:order "desc"}}
       :asc-date {(name a) {:order "asc"}}
       :desc-date {(name a) {:order "desc"}}))
   (partition 2 sort)))

(defn create-elastic-query
  [q]
  (cond-> {}
    (:filter q) (merge {:query {:bool {:filter (parse op->filter-builder (:filter q))}}})
    (:search q) (search-with-filter q)
    (:select q) (assoc :_source (mapv name (:select q)))
    (:sort q) (assoc :sort (form-sorting (:sort q)))))

(defn advanced-direct-search
  [ctx elastic-query & {:keys [raw-data] :as opts}]
  (let [json-query (util/to-json elastic-query)
        index-name (make-index-name (realm ctx) (or (:index-name ctx) (:service-name ctx)))
        {:keys [error] :as body} (elastic/query
                                  {:method         "POST"
                                   :path           (str "/" index-name "/_search")
                                   :body           json-query
                                   :elastic-search (:elastic-search ctx)
                                   :aws            (:aws ctx)})
        total (get-in body [:hits :total :value])]

    (when error
      (throw (ex-info "Elastic query failed" error)))
    (log/debug "Elastic query")
    (log/debug json-query)
    (log/debug body)
    {:total total
     :from  (get elastic-query :from 0)
     :size  (get elastic-query :size default-size)
     :hits
     (if raw-data
       (into []
             (get-in body [:hits :hits] []))
       (mapv
        :_source
        (get-in body [:hits :hits] [])))}))

(defmethod advanced-search
  :elastic
  [{:keys [query] :as ctx}]
  (let [elastic-query (->
                       (create-elastic-query query)
                       (assoc
                        :from (get query :from 0)
                        :size (get query :size default-size)))]
    (advanced-direct-search ctx elastic-query)))

(defn flatten-paths
  ([m separator]
   (flatten-paths m separator []))
  ([m separator path]
   (->> (map (fn [[k v]]
               (if (and (map? v) (not-empty v))
                 (flatten-paths v separator (conj path k))
                 [(->> (conj path k)
                       (map name)
                       (string/join separator)
                       keyword) v]))
             m)
        (into {}))))

(defn create-simple-query
  [query]
  {:pre [query]}
  (util/to-json
   {:size  600
    :query {:bool
            {:must (mapv
                    (fn [[field value]]
                      {:term {(field+keyword field) value}})
                    (seq (flatten-paths query ".")))}}}))

(defmethod simple-search
  :elastic
  [{:keys [query] :as ctx}]
  (log/debug "Executing simple search" query)
  (let [index-name (make-index-name (realm ctx) (:service-name ctx))
        param (dissoc query :query-id)
        body (util/d-time
              "Doing elastic search (Simple-search)"
              (elastic/query
               {:method         "POST"
                :path           (str "/" index-name "/_search")
                :body           (create-simple-query param)
                :elastic-search (:elastic-search ctx)
                :aws            (:aws ctx)}))]
    (mapv
     :_source
     (get-in body [:hits :hits] []))))

(defn store-to-elastic
  [{:keys [aggregate] :as ctx}]
  (loop [count 0]
    (let [index-name (make-index-name (realm ctx) (:service-name ctx))
          {:keys [error]} (elastic/query
                           {:method         "POST"
                            :path           (str "/" index-name "/_doc/" (:id aggregate))
                            :body           (util/to-json aggregate)
                            :elastic-search (:elastic-search ctx)
                            :aws            (:aws ctx)})]
      (if error
        (throw (ex-info "Could not store aggregate" {:error error}))))))

(defmethod update-aggregate
  :elastic
  [{:keys [aggregate] :as ctx}]
  (util/d-time
   (str "Updating aggregate s3: "
        (realm ctx)
        " "
        (:id aggregate)
        " "
        (:version aggregate))
   (s3.vs/store-to-s3 ctx))
  (util/d-time
   (str "Updating aggregate elastic: "
        (realm ctx)
        " "
        (:id aggregate)
        " "
        (:version aggregate))
   (store-to-elastic ctx)))

(defmethod with-init
  :elastic
  [ctx body-fn]
  (log/debug "Initializing")
  (body-fn ctx))

(defn register
  [ctx]
  (assoc ctx
         :view-store :elastic
         :elastic-search {:scheme (util/get-env "IndexDomainScheme" "https")
                          :url    (util/get-env "IndexDomainEndpoint" "127.0.0.1:9200")}))

(defmethod get-snapshot
  :elastic
  [ctx id]
  (util/d-time
   (str "Fetching snapshot aggregate: " (realm ctx) " " id)
   (s3.vs/get-from-s3 ctx id)))

(defmethod get-by-id-and-version
  :elastic
  [ctx id version]
  (when-let [history-entry
             (history/get-by-id-and-version ctx id version)]
    (:aggregate history-entry)))
