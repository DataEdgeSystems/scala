import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xieenze on 17-6-26.
  */
object wc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)

    val input = sc.textFile("/home/xieenze/Desktop/test.txt")
    val lines = input.flatMap(line=>line.split(" "))
    val count=lines.map(word=>(word,1)).reduceByKey{case(x,y)=>x+y}
    val output = count.saveAsTextFile("/home/xieenze/Desktop/op.txt")
  }
}

