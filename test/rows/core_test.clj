(ns rows.core-test
  (:require [clojure.test :refer :all]
            [rows.core :refer :all]
            [clojure.java.io :as io]))

(defn get-test-file [f]
  (-> (str "resources/" f) io/resource io/file))

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

(deftest in-rows-test-simple1
  (testing "in-rows with a simple CSV inputfile"
    (let [rows (in-rows (get-test-file "simple1.csv"))]
      (is (= rows
             (list {"First" "Bradley", "Last" "Geilfuss", "ID" "8"}
                   {"First" "Alan", "Last" "Malloy", "ID" "1"}
                   {"First" "Aaron", "Last" "Crow", "ID" "9"}))))))

(deftest merge-in-rows-test-simples
  (testing "merge-in-rows with 3 simple CSV input files"
    (let [rows (merge-in-rows 
                   (map get-test-file
                        ["simple1.csv" "simple2.csv" "simple3.csv"]))]
      (is (= 10 (count rows)))
      (is (= {"Kevin" 1, "Wes" 1, "Diego" 1,
              "Zach" 1, "Bradley" 1, "Boris" 1,
              "Aaron" 1, "Ainsley" 1, "Alan" 1,
              "Byron" 1}
             (frequencies (map #(get % "First") rows)))))))
