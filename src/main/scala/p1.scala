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
//    spark.sql("SELECT FLOOR(SUM(num_consumers)) AS tot_consumers FROM bev_branch b LEFT JOIN bev_conscount c ON b.Drink=c.Drink WHERE branch='Branch1'").show()
//    spark.sql("SELECT FLOOR(SUM(num_consumers)) AS tot_consumers FROM bev_branch b LEFT JOIN bev_conscount c ON b.Drink=c.Drink WHERE branch='Branch2'").show()

    /* Scenario 2 (UNCOMMENT IF YOU WISH TO RUN)*/
//    spark.sql("WITH t1 AS (" +
//               "SELECT b.Drink, SUM(num_consumers) AS num_orders FROM bev_branch b LEFT JOIN bev_conscount c ON b.Drink=c.Drink " +
//               "WHERE branch='Branch1' GROUP BY b.drink) " +
//               "SELECT drink FROM t1 WHERE num_orders=(SELECT MAX(num_orders) FROM t1)").show()
//
//    spark.sql("WITH t1 AS (" +
//      "SELECT b.Drink, SUM(num_consumers) AS num_orders FROM bev_branch b LEFT JOIN bev_conscount c ON b.Drink=c.Drink " +
//      "WHERE branch='Branch2' GROUP BY b.drink) " +
//      "SELECT drink FROM t1 WHERE num_orders=(SELECT MIN(num_orders) FROM t1)").show()
//
//    spark.sql("WITH t1 AS (" +
//      "SELECT b.Drink, SUM(num_consumers) AS num_orders FROM bev_branch b LEFT JOIN bev_conscount c ON b.Drink=c.Drink " +
//      "WHERE branch='Branch2' GROUP BY b.drink), t2 AS ( " +
//      "SELECT drink, ABS( num_orders - (SELECT AVG(num_orders) FROM t1)) AS mid FROM t1 ) " +
//      "SELECT drink FROM t2 WHERE mid=(SELECT MIN(mid) FROM t2)").show()

    /* Scenario 3 */

//    spark.sql("SELECT DISTINCT(drink) AS drinks FROM bev_branch WHERE branch='Branch1' OR branch='Branch8' OR branch='Branch9' ORDER BY drinks").show()
//    spark.sql("WITH t1 AS (" +
//      "SELECT * FROM bev_branch WHERE branch='Branch4'), t2 AS (" +
//      "SELECT * FROM bev_branch WHERE branch='Branch7') " +
//      "SELECT drink FROM t1 INTERSECT SELECT drink FROM t2 ORDER BY drink").show()

    /* Scenario 4 */
//    spark.sql("CREATE VIEW s3 AS SELECT DISTINCT(drink) AS drinks FROM bev_branch WHERE branch='Branch1' OR branch='Branch8' OR branch='Branch9' ORDER BY drinks")
    spark.sql("SELECT * FROM s3").show()
//    spark.sql("CREATE TABLE branch_part(drink STRING) PARTITIONED BY (branch STRING)")
//    spark.sql("INSERT INTO branch_part SELECT drink, branch FROM bev_branch")
    spark.sql("SELECT * FROM branch_part ORDER BY branch").show()
    spark.sql("SHOW PARTITIONS branch_part").show()

    /* Scenario 5 */
//    spark.sql("CREATE TABLE branch_5(drink STRING, branch STRING)")
//    spark.sql("INSERT INTO TABLE branch_5 SELECT drink, branch FROM bev_branch")
//    spark.sql("ALTER TABLE branch_5 ADD COLUMNS (note STRING, comment STRING)")
    spark.sql("SELECT * FROM branch_5 ORDER BY drink, branch").show()

    /* Scenario 6 */
    spark.sql("SELECT * FROM branch_5 MINUS SELECT * FROM branch_5 WHERE drink='Cold_Coffee' AND branch='Branch6' ORDER BY drink, branch").show()

  }
}