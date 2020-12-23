# csvdb

## Install
Clojure translator and REPL: https://clojure.org/guides/getting_started

Leiningen (build-system): https://leiningen.org/

## Exercise 
Create small database management system on base CSV-files: student.csv, student_subject.csv, subject.csv.
The files implemented link - "many-to-many". You need implement only select-query. For instance:
``` clojure
(select student
    :where #(> (:id %) 1)
    :order-by :year
    :limit 2)

(select student-subject
    :limit 2
    :joins [[:student_id student :id] [:subject_id subject :id]])
```
You need to run these queries in the REPL, so you don't need to write any additional I/o system for the console.

## My implementation:
All implementation in ***core.clj.***

Run in Repl: ***lein repl***<br>
Run tests: ***lein test***
