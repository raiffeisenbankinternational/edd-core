(ns edd.clj.core-test
  (:require
   [clojure.test :refer [deftest is]]
   [edd.clj.core :as clj]))

(deftest test-parse-long

  (let [result (clj/parse-long "-123")]
    (is (= -123 result)))

  (try
    (clj/parse-long nil)
    (is false)
    (catch IllegalArgumentException e
      (is true)
      (is (= "Expected string, got nil"
             (ex-message e)))))

  (let [result (clj/parse-long "dunno")]
    (is (nil? result))))

(deftest test-uuid
  (let [uuid (clj/random-uuid)]
    (is (uuid? uuid))))

(deftest test-abs
  (is (= 42 (clj/abs -42))))

(deftest test-update-keys-vals

  (let [result
        (clj/update-keys {:foo 1 :bar 2} str "_hello")]
    (is (= {":foo_hello" 1 ":bar_hello" 2}
           result)))

  (let [result
        (clj/update-vals {:foo 1 :bar 2} + 10)]
    (is (= {:foo 11 :bar 12}
           result))))

(deftest test-update-each

  (let [result
        (update {:numbers [1 2 3]}
                :numbers
                clj/each
                + 10)]
    (is (= {:numbers [11 12 13]} result)))

  (let [result
        (update {:numbers #{1 2 3}}
                :numbers
                clj/each
                + 10)]
    (is (= {:numbers #{11 12 13}} result))))
