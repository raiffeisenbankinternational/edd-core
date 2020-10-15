(ns edd.test.fixture.seach-test
  (:require [clojure.test :refer :all]
            [edd.search :refer [parse]]
            [lambda.test.fixture.state :as state]
            [edd.test.fixture.search :as search]))

(deftest and-test-1
  (with-redefs [parse (fn [ctx p]
                        (get {:a #(= (:k1 %) 3)
                              :b #(= (:k2 %) 4)}
                             p))]
    (is (= true
           (apply
             (search/and-fn search/mock :a :b)
             [{:k1 3 :k2 4}])))))

(deftest and-test-2
  (with-redefs [parse (fn [ctx p]
                        (get {:a #(= (:k1 %) 5)
                              :b #(= (:k2 %) 6)}
                             p))]
    (is (= false
           (apply
             (search/and-fn search/mock :a :b)
             [{:k1 3 :k2 4}])))))

(deftest and-test-3
  (with-redefs [parse (fn [ctx p]
                        (get {:a #(= (:k1 %) 3)
                              :b #(= (:k2 %) 6)}
                             p))]
    (is (= false
           (apply
             (search/and-fn search/mock :a :b)
             [{:k1 3 :k2 4}])))))

(deftest or-test-1
  (with-redefs [parse (fn [ctx p]
                        (get {:a #(= (:k1 %) 3)
                              :b #(= (:k2 %) 4)}
                             p))]
    (is (= true
           (apply
             (search/or-fn search/mock :a :b)
             [{:k1 3 :k2 4}])))))

(deftest or-test-2
  (with-redefs [parse (fn [ctx p]
                        (get {:a #(= (:k1 %) 5)
                              :b #(= (:k2 %) 6)}
                             p))]
    (is (= false
           (apply
             (search/or-fn search/mock :a :b)
             [{:k1 3 :k2 4}])))))

(deftest or-test-3
  (with-redefs [parse (fn [ctx p]
                        (get {:a #(= (:k1 %) 3)
                              :b #(= (:k2 %) 6)}
                             p))]
    (is (= true
           (apply
             (search/or-fn search/mock :a :b)
             [{:k1 3 :k2 4}])))))

(deftest eq-test-1
  (is (= true
         (apply
           (search/eq-fn
             {}
             "k1"
             3)
           [{:k1 3 :k2 4}]))))

(deftest eq-test-2
  (is (= true
         (apply
           (search/eq-fn
             {}
             "k1.k11"
             3)
           [{:k1 {:k11 3
                  :k12 4}
             :k2 4}]))))

(deftest eq-test-3
  (is (= true
         (apply
           (search/eq-fn
             {}
             ".k1.k11"
             3)
           [{:k1 {:k11 3
                  :k12 4}
             :k2 4}]))))

(deftest eq-test-4
  (is (= false
         (apply
           (search/eq-fn
             {}
             ".k1.k11"
             4)
           [{:k1 {:k11 3
                  :k12 4}
             :k2 4}]))))

(deftest eq-test-5
  (is (= true
         (apply
           (search/eq-fn
             :key "121")
           [{:key "121", :b "be"}]))))

(deftest in-fn-test-1
  (let [in-fn-h (search/in-fn {} :a ["a"])]
    (is (= true
           (in-fn-h {:a "a"
                     :b "b"})))))

(deftest in-fn-test-2
  (let [in-fn-h (search/in-fn {} :a ["c"])]
    (is (= false
           (in-fn-h {:a "a"
                     :b "b"})))))

(deftest in-fn-test-3
  (let [in-fn-h (search/in-fn {} :a ["c" "d" "a"])]
    (is (= true
           (in-fn-h {:a "a"
                     :b "b"})))))

(deftest in-fn-test-4
  (let [in-fn-h (search/in-fn {} "a.c" ["a" "b"])]
    (is (= false
           (in-fn-h {:a {:c "c"}
                     :b "b"})))))

(deftest in-fn-test-5
  (let [in-fn-h (search/in-fn {} "a.c" ["a" "b" "c"])]
    (is (= true
           (in-fn-h {:a {:c "c"}
                     :b "b"})))))

(deftest to-keyword-vector
  (is (= [:a :b]
         (search/to-keywords [:a :b]))))

(deftest to-keyword-mixed-vector
  (is (= [:a :b :c]
         (search/to-keywords [:a "b.c"])))
  (is (= [:a :b :c]
         (search/to-keywords [:a.b.c]))))

(deftest to-keyword-simple-string
  (is (= [:a]
         (search/to-keywords "a"))))

(deftest search-filter-test
  (is (= true
         (search/search-fn
           {:search [:fields [:a "c.d"]
                     :value "ps"]}
           {:a "bakeps"
            :b "lorem"
            :c {:d "ipsum"}}))))

(deftest search-filter-test-not-matching
  (is (= false
         (search/search-fn
           {:search [:fields [:a "c.d"]
                     :value "ket"]}
           {:a "bakeps"
            :b "lorem"
            :c {:d "ipsum"}}))))

(deftest field-to-kw-list-test
  (is (= [:a :b]
         (search/field-to-kw-list "a.b")))
  (is (= [:a :b]
         (search/field-to-kw-list :a.b))))

(deftest select-fn-test
  (is (= {:a "a" :b {:c "d"}}
         (search/select-fn {:select [:a :b.c]}
                           {:a "a"
                            :g "h"
                            :b {:c "d"
                                :e "f"}})))

  (is (= {:a "a" :b {:c "d"}}
         (search/select-fn {:select ["a" "b.c"]}
                           {:a "a"
                            :g "h"
                            :b {:c "d"
                                :e "f"}}))))

(deftest test-compare-items-1
  (is (= -1
         (search/compare-item
           [[[:a] :asc]]
           {:a "a"}
           {:a "b"}))))

(deftest test-asc-sort-items-1
  (is (= [{:a "a"}
          {:a "b"}]
         (search/sort-fn
           {:sort ["a" "asc"]}
           [{:a "a"}
            {:a "b"}]))))

(deftest test-desc-sort-items-2
  (is (= [{:a "b"}
          {:a "a"}]
         (search/sort-fn
           {:sort ["a" "desc"]}
           [{:a "a"}
            {:a "b"}]))))

(deftest test-compare-items-2
  (is (= 0
         (search/compare-item
           [[[:a] "asc"]
            [[:b :c] "desc"]]
           {:a "a"
            :b {:c "ab"}
            :d "e"}
           {:a "a"
            :b {:c "ab"}
            :d "e"}))))

(deftest test-compare-items-3
  (is (= 1
         (search/compare-item
           [[[:a] :asc]
            [[:b :c] :desc]]
           {:a "a"
            :b {:c "1"}
            :d "e"}
           {:a "a"
            :b {:c "2"}
            :d "e"}))))

(deftest test-compare-items-4
  (is (= -1
         (search/compare-item
           [[[:a] :asc]
            [[:b :c] :asc]]
           {:a "a"
            :b {:c "1"}
            :d "e"}
           {:a "a"
            :b {:c "2"}
            :d "e"}))))

(deftest test-compare-items-5
  (is (= -1
         (search/compare-item
           [[[:a] :asc]
            [[:b :c] :asc]]
           {:a "1"
            :b {:c "1"}
            :d "e"}
           {:a "2"
            :b {:c "2"}
            :d "e"}))))

(deftest test-compare-items-6
  (is (= 1
         (search/compare-item
           [[[:a] :asc]
            [[:b :c] :asc]]
           {:a "2"
            :b {:c "1"}
            :d "e"}
           {:a "1"
            :b {:c "2"}
            :d "e"}))))

(deftest test-desc-sort-items-1
  (let [a {:a "2"
           :b {:c "1"}
           :d "1"}
        b {:a "1"
           :b {:c "2"}
           :d "1"}
        c {:a "3"
           :b {:c "3"}
           :d "2"}
        data [a b c]]
    (is (= [c a b]
           (search/sort-fn
             {:sort ["a" "desc"
                     "b.c" "asc"]}
             data)))
    (is (= [b a c]
           (search/sort-fn
             {:sort ["a" "asc"
                     "b.c" "asc"]}
             data)))
    (is (= [a b c]
           (search/sort-fn
             {:sort ["d" "asc"
                     "b.c" "asc"]}
             data)))
    (is (= [b a c]
           (search/sort-fn
             {:sort ["d" "asc"
                     "b.c" "desc"]}
             data)))
    (is (= [c b a]
           (search/sort-fn
             {:sort ["d" "desc"
                     "b.c" "desc"]}
             data)))))

(deftest test-desc-sort-number-1
  (let [a {:a 2
           :d "1"}
        b {:a 12
           :d "1"}
        c {:a 3
           :d "21"}
        data [a b c]]
    (is (= [b c a]
           (search/sort-fn
             {:sort ["a" :desc-number]}
             data)))
    (is (= [a c b]
           (search/sort-fn
             {:sort ["a" :asc-number]}
             data)))
    (is (= [a c b]
           (search/sort-fn
             {:sort ["a" "asc-number"]}
             data)))
    (is (= [b c a]
           (search/sort-fn
             {:sort [:a "desc-number"]}
             data)))))

(deftest exitst-test
  (let [exist-fn (search/exists-fn {} :a.b)]
    (is (= true
           (exist-fn {:a {:b "a"}})))
    (is (= false
           (exist-fn {:a {:c "a"}})))
    (is (= true
           (exist-fn {:a {:b nil}})))))

(deftest exitst-test-2
  (let [data [{:a {:b :e}
               :c :d}
              {:c :d}]]
    (binding [state/*dal-state* (atom {:aggregate-store data})]
      (is (= {:from  0
              :hits  [{:a {:b :e}
                       :c :d}]
              :size  50
              :total 1}
             (search/advanced-search {} {:filter [:exists :a.b]})))
      (is (= {:from  0
              :hits  [{:c :d}]
              :size  50
              :total 1}
             (search/advanced-search {} {:filter [:not [:exists :a]]}))))))