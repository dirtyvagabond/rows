(ns rows.core
  (:require [clojure-csv.core :as csv]
            [cheshire.core :as jsn]
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
; support automatic resume from hard crash
;   - maybe use java.io.LineNumberReader or similar?

; Supported text file formats

(defn sv-formatter [delim]
  (fn [row headers]
    (csv/write-csv [(for [k headers] (get row k ""))] :delimiter delim)))

(defn sv-parser [delim]
  (fn [file]
    (let [[hdr & rows] (csv/parse-csv (io/reader file) :delimiter delim)]
      (for [r rows]
        (zipmap hdr r)))))

(def FORMATTERS
  ;; Formatters that know how to take a row and return a line of text
  {".csv"  (sv-formatter \,)
   ".tab"  (sv-formatter \tab)
   ".tsv"  (sv-formatter \tab)
   ".json" (fn [row _] (format "%s\n" (jsn/generate-string row)))})

(def PARSERS
  ;; Parsers that know how to take a file of a specific format and return
  ;; a sequence of hash-maps representing rows from the file
  {".json" (fn [file]
             (map jsn/parse-string (line-seq (io/reader file))))
   ".csv" (sv-parser \,)
   ".tsv" (sv-parser \tab)})

(defn by-ext [m f]
  (let [ext (fs/extension f)]
    (or (get m (str/lower-case ext))
        (throw (Exception.
                (format "Unrecognized file extension (%s): %s" ext f))))))


; Helpers for file writer management

(defn writer* [m file]
  @(get (swap! m update-in [file] #(or % (delay (io/writer file)))) file))

(defn write* [m file line]
  (.write (writer* m file) line))

(defn close* [m]
  (doseq [[_ w] @m]
    (.close @w)))


(defn output-from-name [n outputs]
  (first (filter #(= n (fs/base-name %)) outputs)))

(defn in-rows [file]
  ((by-ext PARSERS file) file))

(defn merge-in-rows
  "Returns a sequence of all the rows from all the files"
  [files]
  (mapcat in-rows files))

(defn delete [files]
  (doseq [f files] (fs/delete f)))

;; TODO: a header policy that isn't insane
(defn guess-at-headers [rows]
  (distinct (mapcat keys (take 1000 rows))))
(def HEADER-POLICY
  {".json" :no
   ".csv" :yes
   ".tab" :yes
   ".tsv" :yes})
(defn write-headers [writers outputs headers]
  (doseq [outfile outputs]
    (when (= :yes (by-ext HEADER-POLICY outfile))
      (let [fmt (by-ext FORMATTERS outfile)
            line (fmt (into {} (for [h headers] [h h])) headers)]
        (write* writers outfile line)))))

(defn write-rows
  "Writes rows (a sequence of hash-maps) to outputs.

   Each row in rows may be wrapped with metadata indicating the desired output 
   target"
  ([rows outputs headers]
     (let [writers (atom {})]
       (delete outputs)
       (write-headers writers outputs headers)
       (try
         (doseq [row rows]
           ;; row will be a hash-map. it may be wrapped in metadata indicating which 
           ;; output it is meant for.
           (let [outfile (-> row meta :output
                             (output-from-name outputs)
                             (or (first outputs))
                             (doto assert))
                 fmt (by-ext FORMATTERS outfile)
                 line (fmt row headers)]
             (write* writers outfile line)))
         (finally (close* writers)))))
  ([rows outputs]
     (write-rows rows outputs (guess-at-headers rows))))

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
