(ns edd.java-lambda-runtime.core-test
  (:require
   [aws.ctx :as aws-ctx]
   [clojure.test :refer [deftest is use-fixtures]]
   [edd.java-lambda-runtime.core :as core]))

(def cached-aws
  #'core/cached-aws)

(use-fixtures :each
  (fn [f]
    (reset! core/init-cache {})
    (f)
    (reset! core/init-cache {})))

(def fetched-aws
  {:aws-access-key-id "fetched-key"
   :aws-secret-access-key "fetched-secret"
   :aws-session-token "fetched-token"})

(defn fetch-stub
  [calls]
  (fn [ctx]
    (swap! calls inc)
    (assoc ctx :aws fetched-aws)))

(deftest refresh-stores-creds-with-timestamp
  (with-redefs [aws-ctx/init (fetch-stub (atom 0))]
    (let [result
          (core/refresh-aws-creds! {})

          cached
          @core/init-cache]

      (is
       (= fetched-aws
          result))

      (is
       (= fetched-aws
          (:aws cached)))

      (is
       (number? (:aws-fetched-at cached))))))

(deftest empty-cache-fetches
  (let [calls
        (atom 0)]
    (with-redefs [aws-ctx/init (fetch-stub calls)]

      (is
       (= fetched-aws
          (cached-aws {})))

      (is
       (= 1
          @calls)))))

(deftest fresh-cache-skips-fetch
  (let [calls
        (atom 0)

        cached-creds
        {:aws-access-key-id "cached-key"}]
    (reset! core/init-cache
            {:aws cached-creds
             :aws-fetched-at (System/currentTimeMillis)})
    (with-redefs [aws-ctx/init (fetch-stub calls)]

      (is
       (= cached-creds
          (cached-aws {})))

      (is
       (= 0
          @calls)))))

(deftest stale-cache-refetches
  (let [calls
        (atom 0)

        thirty-one-minutes-ago
        (- (System/currentTimeMillis) (* 31 60 1000))]
    (reset! core/init-cache
            {:aws {:aws-access-key-id "stale-key"}
             :aws-fetched-at thirty-one-minutes-ago})
    (with-redefs [aws-ctx/init (fetch-stub calls)]

      (is
       (= fetched-aws
          (cached-aws {})))

      (is
       (= 1
          @calls))

      (is
       (= fetched-aws
          (:aws @core/init-cache))))))
