(ns babaska.update
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]))

(defn convert
  [file]
  (let [build-id 21
        lib (symbol `com.rbinternational.glms/edd-core)
        deps (read-string
              (slurp file))

        global (get-in deps [:deps lib])
        deps (if global
               (assoc-in deps [:deps lib] {:mvn/version (str "1." build-id)})
               deps)
        aliases [:test]

        deps (reduce
              (fn [p alias]
                (if (get-in p [:aliases alias :extra-deps lib])
                  (assoc-in p [:aliases alias :extra-deps lib]
                            {:mvn/version (str "1." build-id)})
                  p))
              deps
              aliases)]
    deps))

(deftest test-deps-update
  (is (= (read-string
          (slurp (io/resource "babaska/deps1-out.edn")))
         (convert (io/resource "babaska/deps1.edn"))))
  (is (= (read-string
          (slurp (io/resource "babaska/deps2-out.edn")))
         (convert (io/resource "babaska/deps2.edn")))))

(let [build-id 21
      lib (symbol `com.rbinternational.glms/edd-core)
      deps (read-string
            (slurp (io/file "deps.edn")))
      global (get-in deps [:deps lib])
      deps (if global
             (assoc-in deps [:deps lib] {:mvn/version (str "1." build-id)})
             deps)
      aliases [:test]
      deps (reduce
            (fn [p alias]
              (if (get-in p [:aliases alias :extra-deps lib])
                (assoc-in p [:aliases alias :extra-deps lib]
                          {:mvn/version (str "1." build-id)})
                p))
            deps
            aliases)]
  (spit "deps.edn" (with-out-str
                     (clojure.pprint/pprint deps))))