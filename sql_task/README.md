#Imported required Libraries
import duckdb
import pandas as pd

#Established in memeory db connection
con = duckdb.connect()

#Imported two tables as SQL tables using any SQL-related Python library.
  
    con.execute("""
    CREATE TABLE products_day1 AS
    SELECT * FROM read_csv_auto('products_day1.csv', header=True);
    """)
    
    con.execute("""
    CREATE TABLE products_day2 AS
    SELECT * FROM read_csv_auto('products_day2.csv', header=True);
    """)

#  Check one of the tables
    print(con.execute("SELECT * FROM products_day1").fetchdf())

--------------------------------------------------------------------Part 1

#Created SQL query to find all the rows that were 
  #added(if all columns for a row in day1 table doesn't macth with day 2 catalog without considering Product_id as primary key) or 
  #removed (if all columns for a row in day2 table doesn't macth with day 1 catalog without considering Product_id as primary key)
  #In the question it was mentioned that "1. Changes in Full Rows (Added or Removed):" which means if there is any change in one of the columns including product_id then we should not treat them as insert or delete 
  #rather that we need to treat that as column level differences

# So in below approach the same applied 
  #Goal: To identify genuine additions and removals between two versions of a product catalog dataset (Products_day1 as the old table, Products_day2 as the new one). 
    #And classify rows as "ADDED" or "REMOVED", while excluding rows that have simply undergone column-level changes.

#Only show:
  #Rows that are completely new (do not exist in the old table in any form).
  #Rows that have been fully removed (do not exist in the new table in any form).

#Exclude:
  #Rows with matching product_id but differing column values → These are updates, handled separately.
  #Rows with different product_id but matching column values → These are ID changes, considered updates, not adds/removes.

#1. Removed Rows
  #We select records from the old table (Products_Tab1) that:
  #Do not have an exact match (by all columns except product_id) in the new table.
  #Do not have a matching product_id in the new table (i.e., it's not just a value update).

    -- Truly removed rows
        -- this will fetch rows those are not having matching columns with day2 table without considering product_id
        SELECT d1.*, 'REMOVED' AS change_type
        FROM products_day1 d1
        WHERE NOT EXISTS (
            SELECT 1 FROM products_day2 d2
            WHERE d1.name = d2.name
              AND d1.category = d2.category
              AND d1.price = d2.price
              AND d1.stock = d2.stock
        )
        -- this condition filter outs the rows those were having matching productid in table 2 by considering those rows as updates
        AND d1.product_id NOT IN (
            SELECT d1a.product_id
            FROM products_day1 d1a
            JOIN products_day2 d2a ON d1a.product_id = d2a.product_id
        )

#2. Added Rows
  #We select records from the new table (Products_Tab2) that:
  #Do not have an exact match (by all columns except product_id) in the old table.
  #Do not have a matching product_id in the old table (i.e., it's not just a value update).

    -- Truly added rows
            -- this will fetch rows those are not having matching columns with day2 table without considering product_id
            SELECT d2.*, 'ADDED' AS change_type
            FROM products_day2 d2
            WHERE NOT EXISTS (
                SELECT 1 FROM products_day1 d1
                WHERE d1.name = d2.name
                  AND d1.category = d2.category
                  AND d1.price = d2.price
                  AND d1.stock = d2.stock
            )
            -- this condition filter outs the rows those were having matching productid in table 2 by considering those rows as updates
            AND d2.product_id NOT IN (
                SELECT d2a.product_id
                FROM products_day2 d2a
                JOIN products_day1 d1a ON d2a.product_id = d1a.product_id
            );


#Assign result set to a dataframe and display it for checking

    added_removed_df = con.execute(added_removed_query).df()
    print(added_removed_df)

#Below is another solution by considering that product_id as primary key for the Product catalog tables and will remain unchanged
#Here in modified_rows CTE 
#we will get product_ids, those are present in day 1 alone as Removed rows and 
product_ids, those are present in day 2 alone as Added rows 

#After Getting all those both kinda IDs with flags added along with , we will then join with day1 table and day 2 table to get full data along with change_type column included in final rows

    added_removed_query = """
      with modified_rows as(
          (SELECT product_id, 'REMOVED' AS change_type FROM products_day1
          EXCEPT
          SELECT product_id, 'REMOVED' AS change_type FROM products_day2)

          UNION ALL

          (SELECT product_id, 'ADDED' AS change_type FROM products_day2
          EXCEPT
          SELECT product_id, 'ADDED' AS change_type FROM products_day1)
      )
      select b.*,change_type from modified_rows a
      join products_day1 b
      on a.product_id = b.product_id

      union all

      select b.*,change_type from modified_rows a
      join products_day2 b
      on a.product_id = b.product_id
    """
    added_removed_df = con.execute(added_removed_query).df()
    print(added_removed_df)

#Below behaves as same as above but with removed redundant codes

    added_removed_query = """
      with modified_rows as(
          --change_type = REMOVED
          (SELECT product_id  FROM products_day1
          EXCEPT
          SELECT product_id FROM products_day2)

          UNION ALL
          -- change_type = 'ADDED'
          (SELECT product_id FROM products_day2
          EXCEPT
          SELECT product_id FROM products_day1)
      )
      select
      *,'REMOVED' as change_type from products_day1
      where product_id in (select product_id from modified_rows )

      union all

      select
      *,'ADDED' as change_type from products_day2
      where product_id in (select product_id from modified_rows)
      order by 1
    """
    added_removed_df = con.execute(added_removed_query).df()
    print(added_removed_df)

#Finally loaded teh result set to CSV file

    #Loading resulted output into csv file
    added_removed_df.to_csv('Added_removed_rows.csv', index=False)

--------------------------------------------------------------------Part 2
#Here the approach the followed is by considering Product_id as primary key and find out list of columns present in table except product_id(PK)
#Then iterate through through all of those above columns and see if any of the row value from table 1 is different from table 2 value(check performs between rows based on Product_id)  and if thats the case then fetch that row changed details alone as old, new values
#FOr each column iteration we get a dataframe generated which will be stored in a list and finally the list items are concatenated vertically, basically appended rows alone.

#All the above iteration performs dynamically based on the list of columns, we have in table1 by considering both tables have identical schema

    column_query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'products_day1'
      AND column_name != 'product_id'
    ORDER BY ordinal_position
    """
    
    columns = [row[0] for row in con.execute(column_query).fetchall()]
    
    print(columns)
    

    #Generating individual dataframes based on each column uniqueness check
    for col in columns:
        query = f"""
            SELECT
                d1.product_id,
                '{col}' AS column_changed,
                CAST(d1.{col} AS TEXT) AS old_value,
                CAST(d2.{col} AS TEXT) AS new_value
            FROM products_day1 d1
            JOIN products_day2 d2 ON d1.product_id = d2.product_id
            WHERE d1.{col} IS DISTINCT FROM d2.{col}
        """
        result = con.execute(query).fetchdf()
        print(result)
        diff_rows.append(result)

        #Appending all the individual dataframes generated from our dynamic sql query ouput
    column_changes_df = pd.concat(diff_rows,ignore_index = True).sort_values(by='product_id')

