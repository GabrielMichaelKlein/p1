import org.apache.spark.sql.SparkSession
object p1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    spark.sql("DROP TABLE bev_branch")
    spark.sql("CREATE TABLE bev_branch (Drink STRING, Branch STRING) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_BranchA.txt' INTO TABLE bev_branch")
    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_BranchB.txt' INTO TABLE bev_branch")
    spark.sql("LOAD DATA LOCAL INPATH 'Input/Bev_BranchC.txt' INTO TABLE bev_branch")
    spark.sql("SELECT * FROM bev_branch").show()
    spark.sql("SELECT COUNT(*) FROM bev_branch WHERE branch='Branch1'").show()
  }
}