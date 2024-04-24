(ns edd.view-store.postgres.attrs
  "
  Functions to work with attributes: parse and compose them,
  check their type, etc.
  "
  (:import
   java.io.PushbackReader)
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [edd.clj.core :as clj]
   [edd.io.core :as edd.io]
   [edd.view-store.postgres.const :as c]))

(defn dimension-cocunut? [service attr]
  (and (= (keyword service) c/SVC_DIMENSION)
       (= (keyword attr) :attrs.cocunut)))

(defn dimension-short-name? [service attr]
  (and (= (keyword service) c/SVC_DIMENSION)
       (= (keyword attr) :attrs.short-name)))

(defn term->cocunut ^String [^String term]
  (let [[cocunut short-name] (str/split term #"/" 2)]
    (if short-name
      cocunut
      term)))

(defn term->short-name ^String [^String term]
  (let [[_ short-name] (str/split term #"/" 2)]
    (if short-name
      short-name
      term)))

(def OS_SYS_ATTRS
  "
  A set of special OpenSearch sub-attributes used
  for type coercion, e.g. keyword, datetime, etc.
  "
  #{:keyword})

(defn attr->path
  "
  Turn an attribute into a vector of keywords.
  "
  [attr]
  (let [items
        (->> (-> attr
                 name
                 (str/split  #"\."))
             (mapv keyword))]

    ;; remove the trailing OS system attributes
    (if (->> items peek (contains? OS_SYS_ATTRS))
      (subvec items 0 (-> items count dec))
      items)))

(defn path->attr [path]
  (->> path
       (map name)
       (str/join \.)
       (keyword)))

(defn attrs->tree
  "
  Turn a seq of attributes into a nested tree
  as follows:

  [:id :version :attrs.history :attrs.user.id] ->

  {:id nil
   :version nil
   :attrs {:history nil
           :user {:id nil}}}

  This thee is used to truncate aggregates when
  reading.
  "
  [attrs]
  (reduce
   (fn [acc path]
     (assoc-in acc path nil))
   {}
   (mapv attr->path attrs)))

(def PATHS_BTREE
  "
  Like the content of the `attrs-btree.edn` file
  but each attribute is tranformed into a path.
  "
  (-> "attrs-btree.edn"
      (io/resource)
      (edd.io/read-edn)
      (clj/update-vals clj/each attr->path)))

(def PATHS_WC
  "
  Like the content of the `attrs-wildcard` file
  but each attribute is tranformed into a path.
  "
  (-> "attrs-wildcard.edn"
      (io/resource)
      (edd.io/read-edn)
      (clj/update-vals clj/each attr->path)))

(defn path-btree?
  "
  True if it's a btree path for a given service.
  "
  ^Boolean [service path]
  (get-in PATHS_BTREE [(keyword service) path]))

(defn path-wildcard? ^Boolean [service path]
  "
  True if it's a wildcard path for a given service.
  "
  (get-in PATHS_WC [(keyword service) path]))

(defn subnode
  "
  Having an arbitrary node and a tree of attributes
  (see attrs->tree above), truncate the node keeping
  only the keys specified in the tree. Unlike get-in,
  takes nested vectors into account (they are proces-
  sed recursively).
  "
  [node tree]
  (cond

    (map? node)
    (let [ks (keys tree)]
      (reduce
       (fn [acc k]
         (if-let [tree-sub (get tree k)]
           (update acc k subnode tree-sub)
           acc))
       (select-keys node ks)
       ks))

    (vector? node)
    (mapv subnode node (repeat tree))

    :else
    nil))
