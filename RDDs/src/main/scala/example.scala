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
    val input = sc.textFile("/home/giannislelekas/Desktop/TU/Supercomputing for Big Data - ET4310/Labs/Lab1/SBD-2018/data/segment/*.csv")
    printf("Count %d\n", input.count)
    
    //for each field extract the corresponding fields with a map(split), using a tab "\t" as a delimiter and considering blanks with "-1".
    val fields = input.map(x => x.split("\t", -1))
    //fields.filter(x=>x(1).contains("20180218"))
    val names = fields.map(x=>x(23)).filter(x=>x != "")
    
    
    // firstly discard the digits pointing to the reference of the name in the article; then separate, filter out blanks and FP and mapReduce. Finally sort and keep 10 highest
    names.map(x=>x.replaceAll("[0-9]","")).flatMap(x=>x.split("[;,]")).filter(x=>x!="" && x!="ParentCategory" && x!="CategoryType" && x!="Type ParentCategory").map(x=>(x,1)).reduceByKey(_+_).map(_.swap).sortByKey(false).map(_.swap).take(10)
    
    //names.flatMap(x=>x.split(";")).map(x=>x.split(",")(0)).map(x=>(x,1)).reduceByKey(_+_).map(_.swap).sortByKey(false).take(10).foreach(println)
    
    //println("Hello")
    sc.stop()
  }
}