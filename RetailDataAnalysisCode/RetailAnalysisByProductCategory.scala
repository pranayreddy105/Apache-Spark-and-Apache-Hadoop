import scala.math.random
import org.apache.spark.sql.SparkSession

object RetailAnalysisByProductCategory {
  def main(args: Array[String]) = {
	System.setProperty("hadoop.home.dir", "G:\\hadoop-2.5.0-cdh5.3.2")
	System.setProperty("spark.sql.warehouse.dir", "G:\\spark-2.0.0-bin-hadoop2.6\\spark-warehouse")
       
      	if (args.length < 2) {
      		System.err.println("Usage: NumberSum <Input-File> <Output-File>");
      		System.exit(1);
    	}
    
    	val spark = SparkSession.builder.appName("RetailDataAnalysis").master("local").getOrCreate()
    	val data = spark.read.textFile(args(0)).rdd
    	val result = data.map { x => { 
      		val tuple = x.split("\\t")
      		(tuple(3),tuple(4).toDouble)
      		}
    	}.reduceByKey(_+_)
    
    	result.saveAsTextFile(args(1))
    	spark.stop()   
  } //End of main Method  
}
