import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._


object SimpleApp {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("PageRank Application")
    val sc = new SparkContext(conf)
    var nIter = args(0).toInt

    time{
        // Create an RDD for the edges and vertices
        val links = sc.textFile("hdfs:///user/leiyang/all-pages-indexed-out.txt", 80).flatMap(getLinks);
        val pages = sc.textFile("hdfs:///user/leiyang/indices.txt", 16).map(getPages);

        //val links = sc.textFile("/Users/leiyang/GitHub/mids/w261_project/HW13/PageRank-test.txt").flatMap(getLinks2);
        //val pages = sc.textFile("/Users/leiyang/GitHub/mids/w261_project/HW13/toy_index.txt").map(getPages2);

        // Build the initial Graph
        val graph = Graph(pages, links);
        // Run pageRank
        val rank = PageRank.run(graph, numIter=nIter).vertices
        // Show results
        println(rank.join(pages).map(l => (l._2._2._1, l._2._1)).sortBy(l=>l._2, ascending=false).take(200).mkString("\n"))
    }
  }

  def getLinks(line: String): Array[Edge[String]] = {
      val elem = line.split("\t", 2)
      for {n <-  elem(1).stripPrefix("{").split(",")
          // get Edge between id
      }yield Edge(elem(0).toLong, n.split(":")(0).trim().stripPrefix("'").stripSuffix("'").toLong, "")
  }

  def getPages(line: String): (VertexId, (String, String)) = {
      val elem = line.split("\t")
      (elem(1).toLong, (elem(0), ""))
  }

  def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block    // call-by-name
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0)/100000000.0/60.0 + " minutes")
      result
  }
}
