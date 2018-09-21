import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object implementationRDD {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val conf = new SparkConf()
        .setAppName("RDDImplementation")
        .setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    // parsing the csv files into input variable 
    val input = sc.textFile("../*.csv")
    printf("Count %d\n", input.count)
    
    //for each field extract the corresponding fields with a map(split), using a tab "\t" as a delimiter and considering blanks with "-1".
    val fields = input.map(x => x.split("\t", -1))
    val tuples = fields.filter(x=>x(23) != "").map(x=>(x(1).substring(0,8),x(23)))
    
    // firstly discard the digits pointing to the reference of the name in the article; then separate, filter out blanks and FP and mapReduce. Finally sort and keep 10 highest
    val topics = tuples.map(x => (x._1,x._2.replaceAll("[0-9]",""))).flatMapValues(x=> x.split("[;,]")).filter(x=>x._2!="" && x._2!="ParentCategory" && x._2!="CategoryType" && x._2!="Type ParentCategory").map(x=>((x._1,x._2),1)).reduceByKey(_+_).map(_.swap).sortByKey(false).map(_.swap).map(x=> (x._1._1,(x._1._2,x._2))).groupByKey().map(x=>Array(x._1,x._2.slice(0,10)))
    topics.foreach(a => println(a.mkString(" ")))
    
    sc.stop()
  }
}
