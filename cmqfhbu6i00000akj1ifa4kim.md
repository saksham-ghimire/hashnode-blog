---
title: "Cross-Row Validation Risk in PostgreSQL CHECK Constraints"
datePublished: 2026-06-15T17:20:49.313Z
cuid: cmqfhbu6i00000akj1ifa4kim
slug: cross-row-validation-risk-in-postgresql-check-constraints
tags: databases

---

Check constraints are widely used to enforce data integrity by validating individual rows during inserts or updates. What many don’t realize is that you can also use custom functions with complex logic as check constraints, greatly increasing their flexibility. However, this added power comes with important caveats and risks that every developer should understand — especially when those functions read data beyond the current row. In this article, we’ll explore these hidden dangers and why caution is crucial.

Let’s start with a simple example: suppose we want to ensure that a value in a column does not appear more than twice in a table. How can we enforce this integrity at the database level? One approach is to use a constraint. Below is a snippet demonstrating a function that attempts to enforce this rule.

```sql
-- create table with 2 field, field_id and price
CREATE TABLE example_table (
    field_id INT,
    price DECIMAL(10, 2)
);

-- custom function that checks if count exceed 2, because constraint are checked at time of insert/update
-- so if it's greater than 1 return false
CREATE OR REPLACE FUNCTION check_field_id_limit(val INT) RETURNS BOOLEAN AS $$
DECLARE
    cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt FROM example_table WHERE field_id = val;
    IF cnt > 1 THEN -- count should never exceed 2
        RETURN FALSE;
    ELSE
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Add custom function as constraint to the table
ALTER TABLE example_table
ADD CONSTRAINT chk_field_id_limit
CHECK (check_field_id_limit(field_id));
```

Looks good, right? Now let’s put this to the test.

```sql
insert into example_table values (1, 100); -- passes
insert into example_table values (1, 200); -- passes
insert into example_table values (1, 200); -- fails
```

The third statement correctly triggers an exception — `Error: new row for relation 'example_table' violates check constraint 'chk_field_id_limit'`—and seemingly fulfills our requirement to restrict entries based on `field_id`. This apparent success, however, masks a serious underlying issue that goes against recommended database practices.

The PostgreSQL documentation offers a stark warning:

> *PostgreSQL does not support* `CHECK` *constraints that reference table data other than the new or updated row being checked. While a* `CHECK` *constraint that violates this rule may appear to work in simple tests, it cannot guarantee that the database will not reach a state in which the constraint condition is false (due to subsequent changes of the other row(s) involved). This would cause a database dump and restore to fail.*

This statement highlights a critical lack of guarantee, but without a concrete example of a breaking case, it often feels abstract. Many of us, myself included, can’t truly grasp the danger until we see it fail. So, let’s move beyond theory and demonstrate precisely how such a constraint can lead to an inconsistent state. We’ll now actively create a scenario where this “guarantee” falls apart.

Consider this scenario involving concurrent transactions

```sql
-- Transaction 1
BEGIN;
INSERT INTO example_table VALUES (2, 100);
INSERT INTO example_table VALUES (2, 200);

-- Transaction 2 (started concurrently, before Transaction 1 commits)
BEGIN;
INSERT INTO example_table VALUES (2, 100);
INSERT INTO example_table VALUES (2, 200);

-- Transaction 1 commits its changes
COMMIT;

-- Transaction 2 commits its changes
COMMIT;
```

Here, the final state of the database is now inconsistent; the initially defined constraint no longer holds validity as selecting on `example_table` We see 4 records of `field_id` 2.

Here’s where the problem arises: At the moment of each `INSERT`, the `CHECK` \`constraint fires. However, because each transaction is operating in `READ COMMITTED` isolation, it can only "see" changes that other transactions have already **committed**. When Transaction 1 inserts its two rows for `field_id = 2,` it doesn't see Transaction 2's uncommitted inserts. The same applies to Transaction 2. Both transactions independently validate their inserts against a partial view of the data, and crucially, both pass the `CHECK` constraint.

Once both transactions successfully commit, the final state of `example_table` is inconsistent. A simple `SELECT` query would reveal four records with`field_id = 2`, completely violating our initially defined constraint that aimed to limit entries for that `field_id`. The database, despite the `CHECK` constraint, has reached an invalid state. Now the backup will fail.

This is precisely why PostgreSQL doesn’t recommend cross-row reads or cross-row validation within `CHECK` constraints. As we've explored, attempting this introduces a fundamental and critical flaw into your database design: while such constraints might deceptively appear to work in simple tests, the inherent nature of concurrent transactions, particularly under default isolation levels, allows them to be bypassed. This leads directly to database inconsistency, where the data violates your intended rules despite the constraint's presence. Consequently, if you are reading cross-row records, you might be exposing your database to unacceptable risks, and proceeding without proper knowledge of isolation levels is strongly not recommended.