(ns rows.core
  (:require [clojure-csv.core :as csv]
            [fs.core :as fs]
            [clojure.string :as str]
            [clojure.java.io :as io]))

; TODO:
; support other formats
; resource mgmt; close robustly on errors for both ins & outs
; proper header support
;   - default output headers should be union of input headers (?)
;   - input files may have .header file instead of inline header
;   - allow user to speicify output headers, per-output
; use slingshot for errors(?)
; support verbose logging level

(def FORMATS
  {".csv" {:reader csv/parse-csv
           :writer csv/write-csv}})

(defn lookup-format [f]
  (let [ext (fs/extension f)]
    (or (get FORMATS (str/lower-case ext))
        (throw (Exception.
                (str "Unrecognized file extension: " ext " (" f ")"))))))

(defn output-from-name [n outputs]
  (first (filter #(= n (fs/base-name %)) outputs)))

(defn in-rows [f]
  (let [[hdr & rows] ((-> f lookup-format :reader) (io/reader f))]
    (for [r rows]
      (apply hash-map (interleave hdr r)))))

(defn merge-in-rows
  "Returns a sequence of all the rows from all the files"
  [files]
  (mapcat in-rows files))

(defn aline [row attrs]
  [(for [k attrs] (or (get row k) ""))])

(defn delete [files]
  (doseq [f files] (fs/delete f)))

(defn write-rows
  "Expects rows to be wrapped with metadata that indicates desired output target"
  [rows outputs attrs]
  (delete outputs)
  (doseq [row rows]
    ;; row will be a hash-map. it may be wrapped in metadata indicating which 
    ;; output it is meant for.
    (let [out (or
               (output-from-name (-> row meta :output) outputs)
               (first outputs))
          _   (assert out)
          fmt (lookup-format out)]
      (spit out ((:writer fmt) (aline row attrs)) :append true))))

(comment
  ;; some testing:
  (def ROWS (merge-in-rows ["test/resources/simple1.csv" "test/resources/simple2.csv" "test/resources/simple3.csv"]))
  (def OUTPUTS ["out.csv"])
  (write-rows ROWS  ["ID" "Last" "First"])

  ;; example client code:
  (defn tout [output row]
    (with-meta row {:output output}))
  (defn a-map-example [rows]
    (map
     (fn [row]
       (condp = (get row "BizType")
         "re" (tout "realEstateAgents.csv" (select-keys row ["Name" "Address" "Tel"]))
         "dr" (tout "doctors.csv" (select-keys row ["Name" "Address" "Tel"]))
         "law" (tout "lawyers.csv" (select-keys row ["Name" "Address" "Tel"]))))
     rows))
)
