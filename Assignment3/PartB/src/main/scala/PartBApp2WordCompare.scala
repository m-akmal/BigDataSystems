// $example on$
//import org.apache.spark.graphx.GraphLoader
// $example off$
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import java.lang.Object
import scala.collection.mutable.ArrayBuffer


object PartBApp2WordCompare {
  def main(args: Array[String]) {

    val file_path: String = {
      if (args.length > 0) {
        args(0)
      }
      else {
        println("No such input file found")
        "vertex_info.txt"
      }
    }

    // Creates a SparkSession.
    val conf = new SparkConf().setAppName("PartBApp2WordComapre")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.task.cpus", "1")

    val sc = new SparkContext(conf)
    val vfile = sc.textFile(file_path)

    val array = vfile.collect.toList
    println(array)

    var count1 = 0
    var edgeString = ""
    var edgesList = ArrayBuffer[Edge[VertexId]]()
    for ( l1<- array ){
      var count2 = 0
      for ( l2<-array){
        if(count1==count2){
          //do nothing
        } else {
          val splitWords1_temp = l1.split("\\s+")
          val splitWords1 = splitWords1_temp.slice(1, splitWords1_temp.length)
          val splitWords2_temp = l2.split("\\s+")
          val splitWords2 = splitWords2_temp.slice(1, splitWords2_temp.length)
          val intersection = splitWords1.intersect(splitWords2)

          if(intersection.length!=0) {
            if(splitWords1.length>splitWords2.length)
            {
              println(count1)
              println(count2)
              println("===GREATER===")
              println(intersection.mkString(" "))
              edgesList += Edge(count1, count2, 1L)
            }
            else
            {
              println(intersection.mkString(" "))
              edgesList += Edge(count1, count2, 2L)
            }
          }
        }
        count2=count2+1

    }
    count1= count1+1
  }

    val edgesRDDFromList = sc.parallelize(edgeString.split("\n"))
    val vertices: RDD[(VertexId, Array[String])] = vfile.map(line => line.split("\\s+")).zipWithIndex().map(line => (line._2, line._1))
    val edges: RDD[Edge[VertexId]] = sc.parallelize(edgesList)
    val g: Graph[Array[String], VertexId] = Graph(vertices, edges)
    println("***Number of Edges where the number of words in the source vertex is strictly larger than the number of words in destination vertex :"+g.edges.filter{ case Edge(src, dst, prop) => prop == 1 }.count)
    sc.stop()
    }
}

// scalastyle:on println
