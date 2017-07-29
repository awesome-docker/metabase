(ns metabase.sync-database.sync-dynamic-test
  "Tests for databases with a so-called 'dynamic' schema, i.e. one that is not hard-coded somewhere.
   A Mongo database is an example of such a DB. "
  (:require [expectations :refer :all]
            [metabase.models
             [database :refer [Database]]
             [field :refer [Field]]
             [table :refer [Table]]]
            [metabase.sync.sync-metadata :as sync-metadata]
            [metabase.test.mock.toucanery :as toucanery]
            [metabase.test.util :as tu]
            [toucan
             [db :as db]
             [hydrate :refer [hydrate]]]
            [toucan.util.test :as tt]))

(defn- get-tables [database-id]
  (->> (hydrate (db/select Table, :db_id database-id, {:order-by [:id]}) :fields)
       (mapv tu/boolean-ids-and-timestamps)))

;; sync-metadata/sync-table-metadata!
(expect
  [[(last toucanery/toucanery-tables-and-fields)]
   [(last toucanery/toucanery-tables-and-fields)]
   [(assoc (last toucanery/toucanery-tables-and-fields)
      :active false
      :fields [])]]
  (tt/with-temp* [Database [{database-id :id, :as db} {:engine :toucanery}]]
    ;; stub out the Table we are going to sync for real below
    (let [table (db/insert! Table
                  :db_id        database-id
                  :name         "transactions"
                  :active       true)]
      [ ;; now lets run a sync and check what we got
       (do
         (sync-metadata/sync-table-metadata! table)
         (get-tables database-id))
       ;; run the sync a second time to see how we respond to repeat syncing (should be same since nothing changed)
       (do
         (sync-metadata/sync-table-metadata! table)
         (get-tables database-id))
       ;; one more time, but lets disable the table this time and ensure that's handled properly
       (do
         (db/update-where! Table {:db_id database-id
                                  :name  "transactions"}
           :active false)
         (sync-metadata/sync-table-metadata! table)
         (get-tables database-id))])))


;; sync-metadata/sync-db-metadata!
(expect
  [toucanery/toucanery-tables-and-fields
   toucanery/toucanery-tables-and-fields
   (conj (vec (drop-last toucanery/toucanery-tables-and-fields))
         (assoc (last toucanery/toucanery-tables-and-fields)
           :active false
           :fields []))]
  (tt/with-temp* [Database [{database-id :id, :as db} {:engine :toucanery}]]
    [ ;; lets run a sync and check what we got
     (do
       (sync-metadata/sync-db-metadata! db)
       (get-tables database-id))
     ;; run the sync a second time to see how we respond to repeat syncing (should be same since nothing changed)
     (do
       (sync-metadata/sync-db-metadata! db)
       (get-tables database-id))
     ;; one more time, but lets disable a table this time and ensure that's handled properly
     (do
       (db/update-where! Table {:db_id database-id
                                :name  "transactions"}
         :active false)
       (sync-metadata/sync-db-metadata! db)
       (get-tables database-id))]))
