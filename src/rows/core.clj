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

(def TSV-FORMAT {:parser #(csv/parse-csv % :delimiter \tab)
                 :formatter #(csv/write-csv % :delimiter \tab)})

(def FORMATS
  {".csv" {:parser csv/parse-csv
           :formatter csv/write-csv}
   ".tab" TSV-FORMAT
   ".tsv" TSV-FORMAT})

(defn lookup-format [f]
  (let [ext (fs/extension f)]
    (or (get FORMATS (str/lower-case ext))
        (throw (Exception.
                (format "Unrecognized file extension (%s): %s" ext f))))))

(defn output-from-name [n outputs]
  (first (filter #(= n (fs/base-name %)) outputs)))

(defn in-rows [file]
  (let [[hdr & rows] ((:parser (lookup-format file)) (io/reader file))]
    (for [r rows]
      (zipmap hdr r))))

(defn merge-in-rows
  "Returns a sequence of all the rows from all the files"
  [files]
  (mapcat in-rows files))

(defn aline [row attrs]
  [(for [k attrs] (get row k ""))])

(defn delete [files]
  (doseq [f files] (fs/delete f)))

(def writers (atom {}))

(defn +writer [file]
  (swap! writers assoc file (io/writer file)))

(defn close* []
  (doseq [[_ w] @writers] (.close w)))

(defn writer* [file]
  (or (get @writers file) (+writer file)))

(defn write-rows
  "Each row in rows may be wrapped with metadata indicating the desired output
   target"
  [rows outputs attrs]
  (do
    (delete outputs)
    (doseq [row rows]
      ;; row will be a hash-map. it may be wrapped in metadata indicating which 
      ;; output it is meant for.
      (let [outfile (-> row meta :output
                        (output-from-name outputs)
                        (or (first outputs))
                        (doto assert))
            fmt (lookup-format outfile)
            line ((:formatter fmt) (aline row attrs))]
        (println "file:" outfile ", wrtr: " (writer* outfile))
        (.write (writer* outfile) line)))
    (close*)))

(comment
  ;; some testing:
  (def ROWS (merge-in-rows ["test/resources/simple1.csv" "test/resources/simple2.csv" "test/resources/simple3.csv"]))
  (def OUTPUTS ["out.csv"])
  (write-rows ROWS OUTPUTS ["ID" "Last" "First"])

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
