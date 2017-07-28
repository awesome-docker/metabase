(ns metabase.driver.generic-sql-test
  (:require [expectations :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.generic-sql :as sql :refer :all]
            [metabase.models
             [field :refer [Field]]
             [table :as table :refer [Table]]]
            [metabase.test.data :refer :all]
            [metabase.test.data.datasets :as datasets]
            [toucan.db :as db])
  (:import metabase.driver.h2.H2Driver))

(def ^:private users-table      (delay (Table :name "USERS")))
(def ^:private venues-table     (delay (Table (id :venues))))
(def ^:private users-name-field (delay (Field (id :users :name))))

(def ^:private generic-sql-engines
  (delay (set (for [engine datasets/all-valid-engines
                    :let   [driver (driver/engine->driver engine)]
                    :when  (extends? ISQLDriver (class driver))]
                (do (require (symbol (str "metabase.test.data." (name engine))) :reload) ; otherwise it gets all snippy if you try to do `lein test metabase.driver.generic-sql-test`
                    engine)))))


;; DESCRIBE-DATABASE
(expect
  {:tables #{{:name "CATEGORIES" :schema "PUBLIC"}
             {:name "VENUES"     :schema "PUBLIC"}
             {:name "CHECKINS"   :schema "PUBLIC"}
             {:name "USERS"      :schema "PUBLIC"}}}
  (driver/describe-database (H2Driver.) (db)))

;; DESCRIBE-TABLE
(expect
  {:name   "VENUES"
   :schema "PUBLIC"
   :fields #{{:name      "NAME",
              :custom    {:column-type "VARCHAR"}
              :base-type :type/Text}
             {:name      "LATITUDE"
              :custom    {:column-type "DOUBLE"}
              :base-type :type/Float}
             {:name      "LONGITUDE"
              :custom    {:column-type "DOUBLE"}
              :base-type :type/Float}
             {:name      "PRICE"
              :custom    {:column-type "INTEGER"}
              :base-type :type/Integer}
             {:name      "CATEGORY_ID"
              :custom    {:column-type "INTEGER"}
              :base-type :type/Integer}
             {:name      "ID"
              :custom    {:column-type "BIGINT"}
              :base-type :type/BigInteger
              :pk?       true}}}
  (driver/describe-table (H2Driver.) (db) @venues-table))

;; DESCRIBE-TABLE-FKS
(expect
  #{{:fk-column-name   "CATEGORY_ID"
     :dest-table       {:name   "CATEGORIES"
                        :schema "PUBLIC"}
     :dest-column-name "ID"}}
  (driver/describe-table-fks (H2Driver.) (db) @venues-table))


;; ANALYZE-TABLE

(expect
  {:row_count 100,
   :fields    [{:id (id :venues :category_id)}
               {:id (id :venues :id)}
               {:id (id :venues :latitude)}
               {:id (id :venues :longitude)}
               {:id (id :venues :name), :values (db/select-one-field :values 'FieldValues, :field_id (id :venues :name))}
               {:id (id :venues :price), :values [1 2 3 4]}]}
  ;; NOCOMMIT
  #_(driver/analyze-table (H2Driver.) @venues-table (set (mapv :id (table/fields @venues-table)))))


;;; FIELD-VALUES-LAZY-SEQ
(datasets/expect-with-engines @generic-sql-engines
  ["Red Medicine"
   "Stout Burgers & Beers"
   "The Apple Pan"
   "Wurstküche"
   "Brite Spot Family Restaurant"]
  (take 5 (#'sql/field-values-lazy-seq datasets/*driver* (db/select-one 'Field :id (id :venues :name)))))


;;; TABLE-ROWS-SEQ
(datasets/expect-with-engines @generic-sql-engines
  [{:name "Red Medicine",                 :price 3, :category_id  4, :id 1}
   {:name "Stout Burgers & Beers",        :price 2, :category_id 11, :id 2}
   {:name "The Apple Pan",                :price 2, :category_id 11, :id 3}
   {:name "Wurstküche",                   :price 2, :category_id 29, :id 4}
   {:name "Brite Spot Family Restaurant", :price 2, :category_id 20, :id 5}]
  (for [row (take 5 (sort-by :id (#'sql/table-rows-seq datasets/*driver*
                                                 (db/select-one 'Database :id (id))
                                                 (db/select-one 'Table :id (id :venues)))))]
    ;; different DBs use different precisions for these
    (-> (dissoc row :latitude :longitude)
        (update :price int)
        (update :category_id int)
        (update :id int))))


;;; Make sure invalid ssh credentials are detected if a direct connection is possible
(expect
  #"com.jcraft.jsch.JSchException:"
  (try (let [engine :postgres
             details {:ssl false,
                      :password "changeme",
                      :tunnel-host "localhost", ;; this test works if sshd is running or not
                      :tunnel-pass "BOGUS-BOGUS-BOGUS",
                      :port 5432,
                      :dbname "test",
                      :host "localhost",
                      :tunnel-enabled true,
                      :tunnel-port 22,
                      :engine :postgres,
                      :user "postgres",
                      :tunnel-user "example"}]
         (driver/can-connect-with-details? engine details :rethrow-exceptions))
       (catch Exception e
         (.getMessage e))))
