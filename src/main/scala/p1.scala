/***************************************************************
 * Gabriel Klein
 * p1
 * August 5, 2021
 *
 * This project practices Spark SQL
 ***************************************************************/

import org.apache.spark.sql.SparkSession
object p1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("WARN")

    /* SQL queries that only need to be ran once */
//    spark.sql("DROP TABLE bev_branch")
//    spark.sql("CREATE TABLE bev_branch (Drink STRING, Branch STRING) row format delimited fields terminated by ','")
//    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_BranchA.txt' INTO TABLE bev_branch")
//    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_BranchB.txt' INTO TABLE bev_branch")
//    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_BranchC.txt' INTO TABLE bev_branch")
//    spark.sql("DROP TABLE bev_conscount")
//    spark.sql("CREATE TABLE bev_conscount (Drink STRING, Num_Consumers INT) row format delimited fields terminated by ','")
//    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_ConscountA.txt' INTO TABLE bev_conscount")
//    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_ConscountB.txt' INTO TABLE bev_conscount")
//    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_ConscountC.txt' INTO TABLE bev_conscount")

    /* Scenario 1 (UNCOMMENT IF YOU WISH TO RUN)*/
//    println("Scenario 1\n")
//    println("Number of consumers from branch 1:")
//    spark.sql("SELECT SUM(num_consumers) AS b1_consumers FROM bev_branch b JOIN " +
//      "bev_conscount c ON b.Drink=c.Drink WHERE branch='Branch2'").show()
//    println("Number of consumers from branch 2:")
//    spark.sql("SELECT SUM(num_consumers) AS b2_consumers FROM bev_branch b JOIN " +
//      "bev_conscount c ON b.Drink=c.Drink WHERE branch='Branch2'").show()
//
//
//    /* Scenario 2 (UNCOMMENT IF YOU WISH TO RUN)*/
//    println("Scenario 2\n")
//    println("Most consumed drink from branch 1:")
//    spark.sql("WITH t1 AS (" +
//               "SELECT b.Drink, SUM(num_consumers) AS num_orders FROM bev_branch b JOIN " +
//      "bev_conscount c ON b.Drink=c.Drink " +
//      "WHERE branch='Branch1' GROUP BY b.drink) " +
//      "SELECT drink FROM t1 WHERE num_orders=(SELECT MAX(num_orders) FROM t1)").show()
//
//    println("Least consumed drink from branch 2:")
//    spark.sql("WITH t1 AS (" +
//      "SELECT b.Drink, SUM(num_consumers) AS num_orders FROM bev_branch b JOIN bev_conscount c ON b.Drink=c.Drink " +
//      "WHERE branch='Branch2' GROUP BY b.drink) " +
//      "SELECT drink FROM t1 WHERE num_orders=(SELECT MIN(num_orders) FROM t1)").show()
//
//    println("Drink that is consumed closest to the average amount from branch 2:")
//    spark.sql("WITH t1 AS (" +
//      "SELECT b.Drink, SUM(num_consumers) AS num_orders FROM bev_branch b JOIN bev_conscount c ON b.Drink=c.Drink " +
//      "WHERE branch='Branch2' GROUP BY b.drink), t2 AS ( " +
//      "SELECT drink, ABS( num_orders - (SELECT AVG(num_orders) FROM t1)) AS mid FROM t1 ) " +
//      "SELECT drink FROM t2 WHERE mid=(SELECT MIN(mid) FROM t2)").show()
//
//    /* Scenario 3 */
//    println("Scenario 3:\n")
//    println("Beverages available on branches 10, 8, and 1:")
//    spark.sql("SELECT DISTINCT(drink) AS drinks FROM bev_branch WHERE " +
//      "branch='Branch1' OR branch='Branch8' OR branch='Branch9' ORDER BY drinks").show()
//
//    println("Beverages common from branches 4 and 7:")
//    spark.sql("WITH t1 AS (" +
//      "SELECT * FROM bev_branch WHERE branch='Branch4'), t2 AS (" +
//      "SELECT * FROM bev_branch WHERE branch='Branch7') " +
//      "SELECT drink FROM t1 INTERSECT SELECT drink FROM t2 ORDER BY drink").show()

    /* Scenario 4 */
    println("Scenario 4\n")
//    spark.sql("CREATE VIEW s3 AS SELECT DISTINCT(drink) AS drinks FROM bev_branch WHERE branch='Branch1' OR branch='Branch8' OR branch='Branch9' ORDER BY drinks")
    spark.sql("SELECT COUNT(*) FROM s3").show()
    spark.sql("SELECT * FROM s3").show()

//    spark.sql("DROP TABLE branch_part")
//    spark.sql("CREATE TABLE branch_part(drink STRING) PARTITIONED BY (branch STRING)")
//    spark.sql("INSERT INTO branch_part SELECT drink, branch FROM bev_branch WHERE branch='Branch1' OR branch='Branch8' OR branch='Branch9'")
    spark.sql("SELECT * FROM branch_part ORDER BY branch").show()
    spark.sql("SHOW PARTITIONS branch_part").show()

//    /* Scenario 5 */
//    println("Scenario 5\n")
////    spark.sql("CREATE TABLE branch_5(drink STRING, branch STRING)")
////    spark.sql("INSERT INTO TABLE branch_5 SELECT drink, branch FROM bev_branch")
////    spark.sql("ALTER TABLE branch_5 ADD COLUMNS (note STRING, comment STRING)")
//    spark.sql("SELECT * FROM branch_5 ORDER BY drink, branch").show()

    /* Scenario 6 */
    println("Scenario 6\n")
    spark.sql("SELECT * FROM branch_5 MINUS SELECT * FROM branch_5 WHERE drink='Cold_Coffee' AND branch='Branch6' ORDER BY drink, branch").show()

    /* Bonus scenarios */
    println("Drinks common to ALL branches:")
    spark.sql("WITH t1 AS (" +
      "SELECT * FROM bev_branch WHERE branch='Branch1'), t2 AS (" +
      "SELECT * FROM bev_branch WHERE branch='Branch2'), t3 AS ( " +
      "SELECT * FROM bev_branch WHERE branch='Branch3'), t4 AS ( " +
      "SELECT * FROM bev_branch WHERE branch='Branch4'), t5 AS ( " +
      "SELECT * FROM bev_branch WHERE branch='Branch5'), t6 AS ( " +
      "SELECT * FROM bev_branch WHERE branch='Branch6'), t7 AS ( " +
      "SELECT * FROM bev_branch WHERE branch='Branch7'), t8 AS ( " +
      "SELECT * FROM bev_branch WHERE branch='Branch8'), t9 AS ( " +
      "SELECT * FROM bev_branch WHERE branch='Branch9') " +
      "SELECT drink FROM t1 INTERSECT SELECT drink FROM t2 " +
      "INTERSECT SELECT drink FROM t3 INTERSECT SELECT drink FROM t4 " +
      "INTERSECT SELECT drink FROM t5 INTERSECT SELECT drink FROM t6 " +
      "INTERSECT SELECT drink FROM t7 INTERSECT SELECT drink FROM t8 " +
      "INTERSECT SELECT drink FROM t9 ORDER BY drink").show()

    println("Least consumed drink from ALL branches:")
    spark.sql("WITH t1 AS (" +
      "SELECT b.Drink, SUM(num_consumers) AS num_orders FROM bev_branch b JOIN bev_conscount c ON b.Drink=c.Drink " +
      "GROUP BY b.drink) " +
      "SELECT drink FROM t1 WHERE num_orders=(SELECT MIN(num_orders) FROM t1)").show()
  }
}