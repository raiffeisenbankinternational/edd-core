(ns edd.test.fixture.search
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.string :as str]
   [edd.search :refer [parse]]
   [edd.search :as search]
   [lambda.test.fixture.state :as state]))

(defn to-keywords
  [a]
  (cond
    (keyword? a) (to-keywords (name a))
    (vector? a) (vec
                 (reduce
                  (fn [v p]
                    (concat v (to-keywords p)))
                  []
                  a))
    :else (map
           keyword
           (remove
            empty?
            (str/split a #"\.")))))

(defn and-fn
  [ctx & r]
  (fn [%]
    (let [result (every?
                  (fn [p]
                    (let [rest-fn (parse ctx p)]
                      (rest-fn %)))
                  r)]
      result)))

(defn or-fn
  [mock & r]
  (fn [%]
    (let [result (some
                  (fn [p]
                    (let [rest-fn (parse mock p)]
                      (rest-fn %)))
                  r)]
      (if result
        result
        false))))

(defn eq-fn
  [_ & [a b]]
  (fn [%]
    (let [keys (to-keywords a)
          response (= (get-in % keys)
                      (case b
                        string? (str/trim b)
                        b))]

      response)))

(defn not-fn
  [mock & [rest]]
  (fn [%]
    (not (apply (parse mock rest) [%]))))

(defn in-fn
  [_ key & [values]]
  (fn [p]
    (let [keys (to-keywords key)
          value (get-in p keys)]
      (if (some
           #(= % value)
           values)
        true
        false))))

(def mock
  {:and and-fn
   :or  or-fn
   :eq  eq-fn
   :not not-fn
   :in  in-fn})

(defn search-fn
  [q p]
  (let [[fields-key fields value-key value] (:search q)]
    (println "SFN")
    (pprint fields)
    (pprint value)
    (if (some
         #(let [v (get-in p (to-keywords %) "")]
            (println v (str (.contains v value)))
            (.contains v value))
         fields)
      true
      false)))

(defn field-to-kw-list
  [p]
  (cond
    (string? p) (map
                 keyword
                 (str/split p #"\."))
    (keyword? p) (map
                  keyword
                  (str/split (name p) #"\."))))

(defn select-fn
  [q %]
  (reduce
   (fn [v p]
     (assoc-in v p
               (get-in % p)))
   {}
   (map
    field-to-kw-list
    (get q :select []))))

(defn get-items
  [q item]
  (reduce
   (fn [p v]
     (str p (get-in item (to-keywords v))))
   ""
   (:sort q)))

(defn compare-as-number
  [a b]
  (let [num_a (if (number? a)
                a
                (Integer/parseInt a))
        num_b (if (number? b)
                b
                (Integer/parseInt b))]
    (compare num_a num_b)))

(defn compare-item
  [attrs a b]
  (println attrs)
  (let [sort (first attrs)
        attribute (first sort)
        order (second sort)
        value_a (get-in a attribute)
        value_b (get-in b attribute)]
    (println attribute)
    (println order)
    (println value_a)
    (println value_b)
    (cond
      (empty? attrs) 0
      (= value_a value_b) (compare-item
                           (rest attrs) a b)
      (= order :asc) (compare value_a value_b)
      (= order :desc) (- (compare value_a value_b))
      (= order :desc-number) (- (compare-as-number value_a value_b))
      (= order :asc-number) (compare-as-number value_a value_b))))

(defn sort-fn
  [q items]
  (sort
   (fn [a b]
     (let [attrs (mapv
                  (fn [[k v]]
                    [(to-keywords k) (keyword v)])
                  (partition 2 (:sort q)))]
       (compare-item attrs a b)))

   items))

(defn advanced-search
  [ctx q]
  (let [state (->> @state/*dal-state*
                   (:aggregate-store))
        apply-filter (if (:filter q)
                       (parse mock (:filter q))
                       (fn [%] true))
        apply-search (if (:search q)
                       (partial search-fn q)
                       (fn [%] true))
        apply-select (if (:select q)
                       (partial select-fn q)
                       (fn [%] %))
        apply-sort (if (:sort q)
                     (partial sort-fn q)
                     (fn [%] %))
        hits (->> state
                  (filter apply-filter)
                  (filter apply-search)
                  (map apply-select)
                  (apply-sort)
                  (into []))
        to (+ (get q :from 0)
              (get q :size (count hits)))]
    {:total (count hits)
     :from  (get q :from 0)
     :size  (get q :size search/default-size)
     :hits  (subvec hits
                    (get q :from 0)
                    (if (> to (count hits))
                      (count hits)
                      to))}))
