import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

class CustomConsoleSink extends StreamSinkProvider {

  // Track the batch id
  private var lastBatchId = -1L

  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
        val batchIdStr = if (batchId <= lastBatchId) {
          s"Rerun batch: $batchId"
        } else {
          lastBatchId = batchId
          s"Batch: $batchId"
        }

        // scalastyle:off println
        println("-------------------------------------------")
        println(batchIdStr)
        println("-------------------------------------------")
        // scalastyle:off println
        data.sparkSession.createDataFrame(
          data.sparkSession.sparkContext.parallelize(data.collect()), data.schema)
          .collect().foreach(println)

        println("Total size= " +data.count())
      }
    }
  }
}

