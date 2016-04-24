import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._


object WikiPageRank {
  def main(args: Array[String]) {

    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("WikiPageRank")
    val sc = new SparkContext(conf)
    var nIter = args(0).toInt

    // Create an RDD for the edges and vertices
    val links = sc.textFile("hdfs:///user/leiyang/all-pages-indexed-out.txt", 80).flatMap(getLinks);
    val pages = sc.textFile("hdfs:///user/leiyang/indices.txt", 16).map(getPages);

    // Build the initial Graph
    val graph = Graph(pages, links);
    // Run pageRank
    val rank = PageRank.run(graph, numIter=nIter).vertices
    rank.cache()
    // Normalize the rank score
    val total = rank.map(l=>l._2).sum()
    val tops = rank.sortBy(l=>l._2, ascending=false).take(200).map(l => (l._1, l._2/total))
    val ret = sc.parallelize(tops).join(pages).map(l => (l._2._2._1, l._2._1)).sortBy(l=>l._2, ascending=false).take(200)
    val elapse = (System.nanoTime()-t0)/1000000000.0/60.0
    // Show results
    println("PageRanking finishes in " + elapse + " minutes!")
    println(ret.mkString("\n"))
  }

  def getLinks(line: String): Array[Edge[String]] = {
      val elem = line.split("\t", 2)
      for {n <-  elem(1).stripPrefix("{").split(",")
          // get Edge between id
      }yield Edge(elem(0).toLong, n.split(":")(0).trim().stripPrefix("'").stripSuffix("'").toLong, "")
  }

  def getPages(line: String): (VertexId, (String, String)) = {
      val elem = line.split("\t")
      return (elem(1).toLong, (elem(0), ""))
  }
}
