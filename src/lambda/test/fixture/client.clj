(ns lambda.test.fixture.client
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [clojure.tools.logging :as log]
            [clojure.data :refer [diff]]))

(def ^:dynamic *world*)

(defn map-body-to-edn
  [traffic]
  (if (:body traffic)
    (try (update traffic :body util/to-edn)
         (catch Exception _e
           traffic))
    traffic))

(defn map-body-to-edn-deep
  [traffic]
  (if (:body traffic)
    (try (-> (update traffic :body util/to-edn)
             (update-in [:body] map-body-to-edn-deep))
         (catch Exception _e
           traffic))
    traffic))

(defmacro verify-traffic-edn
  [y]
  `(is (= ~y
          (->> (:traffic @*world*)
               (mapv map-body-to-edn)
               (mapv #(dissoc % :keepalive))))))

(defmacro verify-traffic
  [y]
  `(is (= ~y
          (mapv
           #(dissoc % :keepalive)
           (:traffic @*world*)))))

(defn traffic-edn
  ([]
   (mapv map-body-to-edn-deep (:traffic @*world*)))
  ([n]
   (nth (traffic-edn) n)))

(defn traffic
  ([]
   (mapv map-body-to-edn (:traffic @*world*)))
  ([n]
   (nth (traffic) n)))

(defn responses []
  (map :body (traffic)))

(defn record-traffic
  [req]
  (let [clean-req (if (= (:method req)
                         :get)
                    (dissoc req :req)
                    req)]
    (swap! *world*
           #(update % :traffic
                    (fn [v]
                      (conj v clean-req))))))

(defn remove-at
  [coll idx]
  (vec (concat (subvec coll 0 idx)
               (subvec coll (inc idx)))))

(defn find-first
  [coll func]
  (first
   (keep-indexed (fn [idx v]
                   (if (func v) idx))
                 coll)))

(defn is-match
  [{:keys [url method body]} v]
  (and
   (= (get v method) url)
   (or (= (:req v) nil)
       (= (get v :req) body)
        ; We want :req to be subset of expected body
       (= (first
           (diff (get v :req)
                 (util/to-edn body)))
          nil))))

(defn handle-request
  "Each request contained :method :url pair and response :body.
  Optionally there might be :req which is body of request that
  has to me matched"
  [{:keys [url method] :as req} & _rest]
  (record-traffic req)
  (let [all (:responses @*world*)
        idx (find-first
             all
             (partial is-match req))
        resp (get all idx)]
    (if idx
      (let [{:keys [reuse-responses]
             :or {reuse-responses false}} (:config @*world*)]
        (when-not reuse-responses
          (swap! *world*
                 update-in [:responses]
                 #(remove-at % idx)))
        (dissoc resp method :req :keep))
      (do
        (log/error {:error {:message "Mock not Found"
                            :url     url
                            :method  method
                            :req     req}})
        {:status 200
         :body   (util/to-json {:result nil})}))))

(defmacro mock-http
  [responses & body]
  `(let [responses# ~responses
         config# (if (map? responses#)
                   (:config responses#)
                   {})
         responses# (if (map? responses#)
                      (:responses responses#)
                      responses#)]
     (binding [*world* (atom {:config config#
                              :responses responses#})]
       (with-redefs [util/request handle-request]
         (do ~@body)))))
