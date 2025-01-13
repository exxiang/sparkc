package sparkc.examples

import org.apache.sparkc.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val inputFile = "data/word.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1))
    wordCount.foreach(println)
  }
}
