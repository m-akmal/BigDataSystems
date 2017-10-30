import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object PartAQuestion1 {
   def main(args: Array[String]) {
   val dirPath: String = args(0)
   val spark = SparkSession.builder.appName("PartAQuestion1")
   .config("spark.driver.memory", "1g")
   .config("spark.cores.max", "6")
   .config("spark.eventLog.enabled","true")
   .config("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
   .config("spark.executor.cores", "4")
   .config("spark.task.cpus", "1")
   .config("spark.sql.streaming.checkpointLocation", "/spark-streaming/checkpoint_1")
   .getOrCreate()

   spark.sparkContext.setLogLevel("ERROR")
   import spark.implicits._

   val userSchema = new StructType().add("userA", "long").add("userB", "long").add("timestamp", "timestamp").add("action", "string")

   val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv(dirPath)

   val wordCounts = csvDF.groupBy(window($"timestamp", "60 minutes", "30 minutes"),$"action").count().orderBy($"window", $"action")
   val query = wordCounts.writeStream.outputMode("complete").format(classOf[CustomConsoleSink].getCanonicalName).start()
   query.awaitTermination()
   }
}

