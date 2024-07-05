(ns edd.xlsx.core
  "
  A thin layer on top of great Fastexcel and Fastexcel-reader libraries
  for writing and reading Excel files. Both compile by GraalVM with no issues.
  As Docjure depedepnds on POI, it is impossible to compile it.

  See:
  - https://github.com/dhatim/fastexcel
  - https://javadoc.io/doc/org.dhatim/fastexcel/latest/
  - https://javadoc.io/doc/org.dhatim/fastexcel-reader/

  "
  (:import
   (clojure.lang Keyword)
   (java.io InputStream
            OutputStream)
   (java.time.temporal Temporal)
   (java.util UUID)
   (org.dhatim.fastexcel StyleSetter
                         Workbook
                         Worksheet)
   (org.dhatim.fastexcel.reader ReadableWorkbook
                                Sheet
                                Row))
  (:require
   [clojure.java.io :as io]))

(set! *warn-on-reflection* true)

;; Setting the right mime type is important because AWS and Outlook
;; can rename the file to *.xls (with no 'x' at the end) after it has
;; been downloaded. It will prevent Excel from opening such a file.
(def MIME-TYPE
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

(defn enumerate [coll]
  (map-indexed vector coll))

(defmulti set-value
  "
  A custom method to write a value into a cell. Mostly used
  to prevent reflection warnings and bypass some corner cases
  (see comments below).
  "
  (fn [_ws _i _j x]
    (type x)))

(defmethod set-value nil
  ;; just do nothing to prevent NPE
  [_ws _i _j _x])

(defmethod set-value UUID
  ;; UUID is not supported by default, add it
  [^Worksheet ws ^Integer i ^Integer j ^UUID x]
  (.value ws i j (str x)))

(defmethod set-value Keyword
  ;; Keyword is not supported by default, add it;
  ;; skip the leading colon.
  [^Worksheet ws ^Integer i ^Integer j ^Keyword x]
  (.value ws i j (-> x str (subs 1))))

(defmethod set-value String
  [^Worksheet ws ^Integer i ^Integer j ^String x]
  (.value ws i j x))

(defmethod set-value Number
  [^Worksheet ws ^Integer i ^Integer j ^Number x]
  (.value ws i j x))

(defmethod set-value Boolean
  [^Worksheet ws ^Integer i ^Integer j ^Boolean x]
  (.value ws i j x))

(defmethod set-value Temporal
  ;; Coerce all the java.time objects to their string view.
  [^Worksheet ws ^Integer i ^Integer j ^Temporal x]
  (.value ws i j (str x)))

(defn write-matrix
  "
  Dump a plain matrix (a vector of vectors) into an output stream.
  The first line is considered a header and has a custom style.
  "
  ([matrix sheet-title output-stream]
   (write-matrix matrix sheet-title output-stream nil))

  ([matrix
    ^String sheet-title
    ^OutputStream output-stream
    {:keys [zoom
            version
            application
            header-color]
     :or {zoom 150
          version "1.0"
          application "clojure"
          header-color "eeeeee"}}]

   (with-open [wb (new Workbook
                       output-stream
                       application
                       version)]

     (let [ws (.newWorksheet wb sheet-title)]

       (.setZoom ws zoom)

       (doseq [[i row] (enumerate matrix)
               [j cell] (enumerate row)]

         (set-value ws i j cell)

         (when (zero? i)
           (-> ws
               ^StyleSetter (.style 0 j)
               ^StyleSetter (.bold)
               ^StyleSetter (.fillColor header-color)
               (.set))))))))

(defn write-matrix-to-file
  "
  Like write-matrix but write into a file by its path.
  "
  ([matrix sheet-title file-path]
   (write-matrix-to-file matrix sheet-title file-path nil))

  ([matrix sheet-title file-path options]
   (with-open [output-stream (-> file-path
                                 io/file
                                 io/output-stream)]
     (write-matrix matrix
                   sheet-title
                   output-stream
                   options))))

(defn read-matrix
  "
  Having an input stream with Excel payload, read the given sheet
  (first when not set) into a matrix, a vector of vectors or strings.
  "
  ([input-stream]
   (read-matrix input-stream nil))

  ([^InputStream input-stream
    {:keys [sheet-index]
     :or {sheet-index 0}}]

   (with-open [wb (new ReadableWorkbook input-stream)]
     (let [^Sheet sheet
           (-> wb (.getSheet sheet-index) (.get))]
       (vec
        (for [^Row row (.read sheet)]
          (let [cell-count (.getCellCount row)]
            (vec
             (for [j (range 0 cell-count)]
               (.orElse (.getCellRawValue row j) nil))))))))))

(defn read-matrix-from-file
  "
  Like read-matrix but read from a file by its path.
  "
  ([file-path]
   (read-matrix-from-file file-path nil))

  ([file-path options]
   (with-open [in (-> file-path
                      io/file
                      io/input-stream)]
     (read-matrix in options))))
