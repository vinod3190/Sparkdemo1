package employee_db

//import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object getemployeedata {
  def main(args: Array[String]): Unit = {
//    val props = ConfigFactory.load()
//    //val envProps = props.getConfig(args(0))
//    val envProps = props.getConfig("dev")
    val spark = SparkSession.
      builder().
      appName("Employee & Department Data")
  .master("local")
    //  master(envProps.getString("execution.mode")).
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
   // spark.conf.set("spark.sql.shuffle.partitions", "2")

    import spark.implicits._

    //val inputBaseDir = envProps.getString("input.base.dir")
    val employee = spark.read.option("header",true)
      .option("inferSchema", true).csv("src/main/resources/Employee.txt")
      //.csv(inputBaseDir +"/employee.txt")

    val Dept = spark.read.option("header",true)
      .option("inferSchema", true).csv("src/main/resources/Department.txt")
     //.csv(inputBaseDir +"/Dept.txt")

    employee.join(Dept,"Dept_id").select( "*").distinct().orderBy("Dept_id")
    //val outputBaseDir = envProps.getString("output.base.dir")
    .show()
//      write.
//      mode("overwrite").
//      json(outputBaseDir + "/target_data")
  }
}
