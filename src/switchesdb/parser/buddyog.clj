(ns switchesdb.parser.buddyog
  "Parser for BuddyOG Topre force curve data.
   BuddyOG CSV format has: Millis, Steps, Travel(mm), Scale raw, Scale(grams)
   We extract: column 3 (Travel) as displacement, column 5 (Scale grams) as force.
   BuddyOG data only has downstroke (no upstroke data)."
  (:refer-clojure :exclude [force])
  (:require [clojure.string :as str]
            [babashka.fs :as fs]
            [clojure.java.io :as io]
            [clojure.data.csv :refer [read-csv]]
            [switchesdb.shared :refer [file-postfix]]))

;; BuddyOG CSV columns (0-indexed):
;; 0: Millis
;; 1: Steps
;; 2: Travel (mm) - displacement
;; 3: Scale raw
;; 4: Scale (grams) - force

(def csv-headers ["displacement" "force" "stroke"])

(defn displacement [v] (nth v 0))
(defn force [v] (nth v 1))
(defn stroke [v] (nth v 2))
(defn measurement [displacement force stroke] [displacement force stroke])

(defn data-row?
  [v]
  (and (number? (displacement v))
       (number? (force v))
       (= "down" (stroke v))))

(defn ignore? [v]
  (< (force v) 10))

(defn deduct [margin v]
  (measurement (- (displacement v) margin) (force v) (stroke v)))

(defn builder-downstroke
  "Builder for downstroke-only data (BuddyOG doesn't have upstroke).
   Similar to commons/builder but handles single stroke."
  [downstroke & {:keys [filename]}]
  (let [downstroke (drop-while (complement data-row?) downstroke)
        _ (assert (seq downstroke) (str "No valid downstroke data in " filename))
        margin (displacement (last (take-while ignore? downstroke)))
        margin? (and (some? margin) (>= (abs margin) 0.01))]
    (when margin?
      (println "INFO Adjusted" filename "by" (- margin) "mm"))
    (cond->> (take-while data-row? (drop-while ignore? downstroke))
      margin? (map (partial deduct margin))
      :always (remove (comp neg? displacement)))))

(defn writer
  [data target-dir filename]
  (let [target (io/file target-dir filename)
        existed? (.exists target)]
    (if existed?
      (println "WARNING Overwriting:" (str target))
      (println "Writing:" (str target)))
    (with-open [file-writer (io/writer target)]
      (clojure.data.csv/write-csv file-writer
                                   (cons csv-headers data)))
    (if existed? :overwritten :written)))

(defn target-filename
  "Generate output filename from switch directory name and datalog number.
   Format: SwitchName_NN~BO.csv where NN is zero-padded datalog number."
  [switch-name datalog-num]
  (let [clean-name (-> switch-name
                       (str/replace #" " "_")
                       (str/replace #"\(" "_")
                       (str/replace #"\)" "_"))
        num-str (format "%02d" datalog-num)]
    (str clean-name "_" num-str (file-postfix :buddyog))))

(defn reader
  "Parse BuddyOG CSV file. Extracts Travel(mm) and Scale(grams) columns."
  [file-reader csv-path switch-name datalog-num]
  (let [values (read-csv file-reader)
        filename (target-filename switch-name datalog-num)
        ;; Skip header row, extract columns 2 (travel) and 4 (scale grams)
        downstroke (map (fn [row]
                          (let [travel (nth row 2 nil)
                                scale (nth row 4 nil)]
                            [(when travel (parse-double travel))
                             (when scale (parse-double scale))
                             "down"]))
                        (drop 1 values))]
    (builder-downstroke downstroke :filename filename)))

(defn parse
  "Parse all BuddyOG CSV files from resources/buddyog directory."
  [target-dir]
  (let [buddyog-dir "resources/buddyog"
        switch-dirs (filter fs/directory? (fs/list-dir buddyog-dir))
        results (atom [])]
    (doseq [switch-dir switch-dirs]
      (let [switch-name (fs/file-name switch-dir)
            ;; Skip hidden directories
            _ (when (str/starts-with? switch-name ".")
                (println "Skipping hidden directory:" switch-name))
            ;; Find all DataLog CSV files (both DataLog_*.csv and *-DataLog_*.csv patterns)
            csv-files (sort (fs/glob switch-dir "*DataLog*.csv"))]
        (when-not (str/starts-with? switch-name ".")
          (loop [files csv-files
                 datalog-num 1]
            (when (seq files)
              (let [csv-path (first files)
                    filename (target-filename switch-name datalog-num)]
                (try
                  (with-open [file-reader (io/reader (fs/file csv-path))]
                    (let [data (reader file-reader csv-path switch-name datalog-num)
                          result (writer data target-dir filename)]
                      (swap! results conj result)))
                  (catch Throwable e
                    (println "ERROR Parsing CSV file" (fs/file-name csv-path)
                             "resulted in exception:" (ex-message e))
                    (swap! results conj :invalid))))
              (recur (rest files) (inc datalog-num)))))))
    (assoc (frequencies @results)
           :filecount (count @results))))
