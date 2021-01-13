(ns lambda.test.fixture.client
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [org.httpkit.client :as http]))

(def ^:dynamic *world*)

(defn map-body-to-json
  [traffic]
  (if (:body traffic)
    (assoc traffic
      :body
      (util/to-edn (:body traffic)))
    traffic))

(defmacro verify-traffic-json
  [y]
  `(is (= ~y
          (mapv
            map-body-to-json
            (:traffic @*world*)))))

(defmacro verify-traffic
  [y]
  `(is (= ~y
          (:traffic @*world*))))

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
        (= (get v :req) (util/to-edn body)))))

(defn handle-request
  [{:keys [url method] :as req} & rest]
  (record-traffic req)
  (let [all (:responses @*world*)
        idx (find-first
              all
              (partial is-match req))
        resp (get all idx)]
    (if idx
      (do
        (swap! *world*
               update-in [:responses]
               #(remove-at % idx))

        (ref
          (dissoc resp method :req :keep)))
      (ref
        {:error {:message "Mock not Found"
                 :url     url
                 :method  method
                 :req     req}}))))

(defmacro mock-http
  [responses & body]
  `(binding [*world* (atom {:responses ~responses})]
     (with-redefs [http/request handle-request]
       (do ~@body))))


