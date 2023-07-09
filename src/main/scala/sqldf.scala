import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object sqldf {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Spark SQL").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")


    val spark = SparkSession.builder().getOrCreate()

    //Read file into a RDD
    val rdd = sc.textFile("file:///D:/Projects/Data/txs.csv")
    //Verify if RDD is created and take only 5 records
    rdd.take(5).foreach(println)

    //Converting RDDofString to Array of String and also remove the delimiter
    val rdd1 = rdd.map(x => x.split(","))

    //Type Conversion
    val rdd2 = rdd1.map(x => (x(0).toInt, x(1), x(2), x(3).toDouble, x(4)))
    rdd2.take(5).head
    //Convert rdd2 into DF
    import spark.implicits._
    //val df = rdd2.toDF()    // Check the Schema 1st with this command then with below command.
    val df = rdd2.toDF("txnID", "txnDate", "custID", "amount", "category")
    df.show()
    df.printSchema()
  }
}
