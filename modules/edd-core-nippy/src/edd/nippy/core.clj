(ns edd.nippy.core
  "
  A custom nippy-based binary encoder and decoder.

  In PLC3, we had troubles when serializing vast Clojure
  collections. Apparently, we faced an issue when nippy
  tries to allocate more than `Integer/MAX_VALUE`
  bytes for an array, which ends up with an exception.

  Another issue that nippy suffers from, is that items
  cannot be read lazily one by one (technically they can
  but there is no such an API). This is important when
  processing items sequentially without reading all them
  at once.

  The encoder below is quite simple. It accepts an arbitrary
  collection (lazy as well) and a destination which gets
  coerced into a `DataOutputStream`. Then, each item gets
  binary-encoded and stored into that stream. The binary
  payload forms a pattern:

  [oid][data][oid][data]     [oid][data]
  |---------||---------| ... |---------|
     item1      item2           itemN

  Where:
  - oid is a byte indicating a type in nippy;
  - data is a byte array whose length and content depends
    on a value type.

  Since nippy relies on `DataOutputStream` to store primitives,
  most of them can be read back by calling `.readInt`, `.readFloat`,
  and other methods from a `DataInputStream` instance.

  The decoding function returns a lazy sequence of items decoded
  back. Since it requires a stream, always place it into the
  `(with-open ...)` macro and ensure you don't process items
  after you have exited from it.

  See examples in the development section below.
  "
  (:require
   [clojure.java.io :as io]
   [taoensso.nippy :as nippy])
  (:import
   clojure.lang.RT
   java.io.DataInputStream
   java.io.DataOutputStream
   java.io.EOFException
   java.io.InputStream
   java.util.zip.GZIPInputStream
   java.util.zip.GZIPOutputStream))

(set! *warn-on-reflection* true)

(defn encode-seq
  "
  Encode a (lazy) sequence of items into a destination
  which can be a file, a string path, a stream, etc.
  Return a number of items processed.
  "
  ^Long [dest coll]
  (with-open [out (-> dest
                      io/output-stream
                      DataOutputStream.)]
    (let [iter (RT/iter coll)]
      (loop [total 0]
        (if (.hasNext iter)
          (let [item (.next iter)]
            (nippy/freeze-to-out! out item)
            (recur (inc total)))
          total)))))

(defn encode-seq-gzip
  "
  Like `encode-seq` but wraps the destination with
  GZIPOutputStream.
  "
  ^Long [dest coll]
  (with-open [out (-> dest
                      io/output-stream
                      GZIPOutputStream.)]
    (encode-seq out coll)))

(defn decode-seq
  "
  For a input stream produced by the `encode-seq` function,
  return a *lazy* sequence of decoded items. Use it under
  the `(with-open ...)` macro.
  "
  [^InputStream input-stream]

  (let [step
        (fn -self [dis]
          (lazy-seq
           (let [item
                 (try
                   (nippy/thaw-from-in! dis)
                   (catch EOFException _
                     ::EOF))]
             (when-not (= item ::EOF)
               (cons item (-self dis))))))]

    (step (new DataInputStream input-stream))))

(defn decode-seq-gzip
  "
  Like `decode-seq` but wraps the input with
  GZIPInputStream.
  "
  [^InputStream input-stream]
  (let [in (-> input-stream
               GZIPInputStream.)]
    (decode-seq in)))

(defn encode-val
  "
  Encode a single value in a destination (file, byte array,
  output stream, etc).
  "
  [dest x]
  (with-open [out (-> dest
                      io/output-stream
                      DataOutputStream.)]
    (nippy/freeze-to-out! out x)))

(defn decode-val
  "
  Decode a single value from a source (file, byte array,
  input stream, etc).
  "
  [src]
  (with-open [in (-> src
                     io/input-stream
                     DataInputStream.)]
    (nippy/thaw-from-in! in)))

(defn encode-val-gzip
  "
  Like `encode-val` but compress the output with Gzip.
  "
  [dest x]
  (with-open [out (-> dest
                      io/output-stream
                      GZIPOutputStream.)]
    (encode-val out x)))

(defn decode-val-gzip
  "
  Like `decode-val` but wraps the input with Gzip.
  "
  [src]
  (with-open [in (-> src
                     io/input-stream
                     GZIPInputStream.)]
    (decode-val in)))

(comment

  (encode-seq "test.nippy"
              (for [x (range 0 20)]
                {:x x :foo "hello"}))

  (with-open [in (io/input-stream "test.nippy")]
    (doseq [item (decode-seq in)]
      (println item)))

  nil)
