(ns edd.nippy.core-test
  (:import
   java.io.File
   java.io.ByteArrayOutputStream)
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [is deftest testing]]
   [edd.nippy.core :as nippy]))

(defn get-temp-file
  (^File []
   (get-temp-file "tmp" ".tmp"))
  (^File [prefix suffix]
   (File/createTempFile prefix suffix)))

(deftest test-encode-decode-seq-ok

  (let [file (get-temp-file "tmp" ".nippy")]

    (testing "test encode"
      (let [amount
            (nippy/encode-seq file
                              (range 1 6))

            out
            (new ByteArrayOutputStream)

            _
            (io/copy (io/file file) out)]

        (is (= [100 1
                100 2
                100 3
                100 4
                100 5]
               (-> out .toByteArray vec)))
        (is (= 5 amount))))

    (testing "test decode"
      (let [items
            (with-open [in (io/input-stream file)]
              (vec (nippy/decode-seq in)))]
        (is (= [1 2 3 4 5]
               (vec items)))))))

(deftest test-encode-decode-seq-gzip-ok

  (let [file (get-temp-file "tmp" ".nippy")]

    (testing "test encode"
      (let [amount
            (nippy/encode-seq-gzip file
                                   (range 1 6))

            buf
            (-> file io/input-stream .readAllBytes)]

        (is (= [31 -117 8 0 0 0 0 0 0 -1 75 97 76 97 74 97 78 97 73 97 5 0 111 -62 -80 -7 10 0 0 0]
               (vec buf)))
        (is (= 5 amount))))

    (testing "test decode"
      (let [items
            (with-open [in (io/input-stream file)]
              (vec (nippy/decode-seq-gzip in)))]
        (is (= [1 2 3 4 5]
               (vec items)))))))

(deftest test-decode-lazy
  (with-open [in (-> [-1 -1 -1 -1 -1]
                     (byte-array)
                     io/input-stream)]
    (let [items (nippy/decode-seq in)]
      (is true "no exception")
      (is (instance? clojure.lang.LazySeq items))
      (try
        #_:clj-kondo/ignore
        (first items)
        (is false)
        (catch Exception e
          (is (= "Thaw failed against type-id: -1"
                 (ex-message e))))))))

(deftest test-encode-decode-val-ok
  (let [file (get-temp-file "tmp" ".nippy")]
    (let [res (nippy/encode-val file {:foo 42})]
      (is (nil? res)))
    (let [res (nippy/decode-val file)]
      (is (= {:foo 42} res)))))

(deftest test-encode-decode-val-gzip-ok
  (let [file (get-temp-file "tmp" ".nippy")]
    (let [res (nippy/encode-val-gzip file {:foo 42})]
      (is (nil? res)))
    (let [buf (-> file io/input-stream .readAllBytes)]
      (is (= [31 -117 8 0 0 0 0 0 0 -1 43 96 -52 98 78 -53 -49 79 -47 2 0 48 56 -60 -97 9 0 0 0]
             (vec buf))))
    (let [res (nippy/decode-val-gzip file)]
      (is (= {:foo 42} res)))))
