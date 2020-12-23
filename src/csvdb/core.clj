(ns csvdb.core
  (:require [clojure-csv.core :as csv]))

(defn- parse-int [int-str]
  (Integer/parseInt int-str))


(def student-tbl (csv/parse-csv (slurp "student.csv")))
(def subject-tbl (csv/parse-csv (slurp "subject.csv")))
(def student-subject-tbl (csv/parse-csv (slurp "student_subject.csv")))

;; (table-keys student-tbl)
;; => [:id :surname :year :group_id]
(defn table-keys [tbl]
  (->> tbl
       (first)
       (map keyword)
       (vec)))

;; (key-value-pairs [:id :surname :year :group_id] ["1" "Ivanov" "1996"])
;; => (:id "1" :surname "Ivanov" :year "1996")
(defn key-value-pairs [tbl-keys tbl-record]
  (flatten (map list tbl-keys tbl-record)))

;; (data-record [:id :surname :year :group_id] ["1" "Ivanov" "1996"])
;; => {:surname "Ivanov", :year "1996", :id "1"}
(defn data-record [tbl-keys tbl-record]
  (apply hash-map (key-value-pairs tbl-keys tbl-record)))

;; (data-table student-tbl)
;; => ({:surname "Ivanov", :year "1996", :id "1"}
;;     {:surname "Petrov", :year "1996", :id "2"}
;;     {:surname "Sidorov", :year "1997", :id "3"})
(defn data-table [tbl]
  (let [keys (table-keys tbl)]
    (map #(data-record keys %) (next tbl))))

;; (str-field-to-int :id {:surname "Ivanov", :year "1996", :id "1"})
;; => {:surname "Ivanov", :year "1996", :id 1}
(defn str-field-to-int [field rec]
  (->> rec
       (field)
       (parse-int)
       (assoc rec field)))

(def student (->> (data-table student-tbl)
                  (map #(str-field-to-int :id %))
                  (map #(str-field-to-int :year %))))

(def subject (->> (data-table subject-tbl)
                  (map #(str-field-to-int :id %))))

(def student-subject (->> (data-table student-subject-tbl)
                          (map #(str-field-to-int :subject_id %))
                          (map #(str-field-to-int :student_id %))))


;; (where* student (fn [rec] (> (:id rec) 1)))
;; => ({:surname "Petrov", :year 1997, :id 2}
;;     {:surname "Sidorov", :year 1996, :id 3})
(defn where* [data condition-func]
  (if-not (nil? condition-func)
    (filter condition-func data)
    data))

;; (limit* student 1)
;; => ({:surname "Ivanov", :year 1998, :id 1})
(defn limit* [data lim]
  (if-not (nil? lim)
    (take lim data)
    data))

;; (order-by* student :year)
;; => ({:surname "Sidorov", :year 1996, :id 3}
;;     {:surname "Petrov", :year 1997, :id 2}
;;     {:surname "Ivanov", :year 1998, :id 1})
(defn order-by* [data column]
  (if-not (nil? column)
    (sort-by column data)
    data))

;; (join* (join* student-subject :student_id student :id) :subject_id subject :id)
;; => [{:subject "Math", :subject_id 1, :surname "Ivanov", :year 1998, :student_id 1, :id 1}
;;     {:subject "Math", :subject_id 1, :surname "Petrov", :year 1997, :student_id 2, :id 2}
;;     {:subject "CS", :subject_id 2, :surname "Petrov", :year 1997, :student_id 2, :id 2}
;;     {:subject "CS", :subject_id 2, :surname "Sidorov", :year 1996, :student_id 3, :id 3}]
(defn join* [data1 column1 data2 column2]
  (let [map-data1-rec (fn [rec data]
                        (->> data
                             (filter #(= (column1 rec) (column2 %)))
                             (map #(merge % rec))))]
    (reduce #(concat %1 (map-data1-rec %2 data2)) '() data1))
  )

;; (perform-joins student-subject [[:student_id student :id] [:subject_id subject :id]])
;; => [{:subject "Math", :subject_id 1, :surname "Ivanov", :year 1998, :student_id 1, :id 1}
;;     {:subject "Math", :subject_id 1, :surname "Petrov", :year 1997, :student_id 2, :id 2}
;;     {:subject "CS", :subject_id 2, :surname "Petrov", :year 1997, :student_id 2, :id 2}
;;     {:subject "CS", :subject_id 2, :surname "Sidorov", :year 1996, :student_id 3, :id 3}]
(defn perform-joins [data joins*]
  (loop [data1 data
         joins joins*]
    (if (empty? joins)
      data1
      (let [[col1 data2 col2] (first joins)]
        (recur (join* data1 col1 data2 col2)
               (next joins))))))

;; Base function
(defn select [data & {:keys [where limit order-by joins]}]
  (-> data
      (perform-joins joins)
      (where* where)
      (order-by* order-by)
      (limit* limit)))