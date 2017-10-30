import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType

object PartAQuestion3 {
  //TODO: Add reading static files and join
   def main(args: Array[String]) {
   val dirPath: String = args(0)
   val inputListPath : String = args(1)
   val spark = SparkSession.builder.appName("PartAQuestion3")
   .config("spark.driver.memory", "1g")
   .config("spark.cores.max", "6")
   .config("spark.eventLog.enabled","true")
   .config("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
   .config("spark.executor.cores", "4")
   .config("spark.task.cpus", "1")
   .config("spark.sql.streaming.checkpointLocation", "/spark-streaming/checkpoint_3")
   .getOrCreate()

   //spark.sparkContext.setCheckpointDir(".")

   spark.sparkContext.setLogLevel("ERROR")

   import spark.implicits._
   //val userList = spark.sparkContext.textFile(inputListPath)
   val userList = spark.read.text(inputListPath).map(r => r(0).asInstanceOf[String].toLong).withColumnRenamed("value", "targetUser")

   userList.collect().foreach(println);
   userList.printSchema()

   val userSchema = new StructType().add("userA", "long").add("userB", "long").add("timestamp", "timestamp").add("action", "string")

   val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv(dirPath)

   val wordCounts = csvDF.join(userList, csvDF.col("userA").equalTo(userList.col("targetUser"))).groupBy("userA").count()

   // val length: Long = (wordCounts.groupBy().count().writeStream.start()).first().get(0).asInstanceOf[Long]


   val query = wordCounts.writeStream.trigger(ProcessingTime(5000)).format(classOf[CustomConsoleSink].getCanonicalName).outputMode("complete").start()
   // val query = wordCounts.writeStream.trigger(ProcessingTime(5000)).format("console").option("numRows", length).outputMode("complete").start()
   query.awaitTermination()
   }
}

