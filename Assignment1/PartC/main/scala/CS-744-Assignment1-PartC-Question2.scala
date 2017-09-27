import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.RangePartitioner

object PageRankPartC2 {
  def main(args: Array[String]) {

    val file_path: String = {
    	if (args.length > 0) {  args(0) }
    	else { "/spark/deployment/web-BerkStan.txt" }
    }
    val num_of_iterations: Int = {
    	if(args.length > 1) { args(1).toInt }
	else { 10 }
    }
    val num_of_partitions: Int = {
    	if(args.length > 1) args(2).toInt
	else { 50 }
    }

    // println(file_path)
    // println(num_of_iterations)
    // println(num_of_partitions)

    val conf = new SparkConf().setAppName("CS-744-Assignment1-PartC-2")

    val sc = new SparkContext(conf)

    val datafile = sc.textFile(file_path, num_of_partitions)

    val raw_data = datafile.filter(f => !f.startsWith("#"))
    // raw_data.count

    val data = raw_data.map(line => line.split("\\s+"))

    // Remove partial data and self loops
    val filtered_data = data.filter(e => (e.size == 2 && !e(0).equals(e(1))))
    // filtered_data.count

    val datatuples = filtered_data.map(e => (e(0), List(e(1))))
    // datatuples.count
    // val rangePartitioner = new RangePartitioner(15, datatuples)

    var init_page_ranks = filtered_data.flatMap(e => List(e(0), e(1))).distinct.map(f => (f, 1.0))
    
    val rankPartitioner = new RangePartitioner(num_of_partitions, datatuples)
    init_page_ranks = init_page_ranks.partitionBy(rankPartitioner)
    var page_ranks = init_page_ranks
    // page_ranks.count
    
    val partitioned_datatuples = datatuples.partitionBy(rankPartitioner)

    val groupedData = partitioned_datatuples.reduceByKey((a,b) => a.:::(b))

    def generateContrib(rank: Double, neighbours: Option[List[String]]) = {
       val new_rank: Double = neighbours match {
         case Some(value) => 1.0*rank/neighbours.get.size
         case None => 0.0
       }
       neighbours.getOrElse(List()).map(n => (n, new_rank))
    }

    val exploded_contribs = page_ranks.leftOuterJoin(groupedData).flatMap(e => generateContrib(e._2._1, e._2._2)).partitionBy(rankPartitioner)

    page_ranks = exploded_contribs.reduceByKey((a,b) => (a+b)).mapValues(v => (0.15 + 0.85*v))

    for (x <- 1 until num_of_iterations) {
      val exploded_contribs = page_ranks.leftOuterJoin(groupedData).flatMap(e => generateContrib(e._2._1, e._2._2)).partitionBy(rankPartitioner)
      page_ranks = exploded_contribs.reduceByKey(_+_).mapValues(v => 0.15 + 0.85*v)
    }

    // page_ranks.count

    val final_page_ranks = init_page_ranks.leftOuterJoin(page_ranks).map(e => {
      val rank = e._2._2 match {
        case Some(value) => value
        case None => e._2._1
      }

      (e._1, rank)
    })

    final_page_ranks.count
    final_page_ranks.take(100).foreach(println)
    sc.stop()
  }
}
