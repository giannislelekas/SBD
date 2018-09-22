import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object GDeltRDD {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val conf = new SparkConf()
        .setAppName("RDDImplementation")
        .setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    // parsing the csv files into input variable 
    val input = sc.textFile("../*.csv")
    
    // for each field extract the corresponding fields with a map(split), using a tab "\t" as a delimiter and considering blanks with "-1".
    val fields = input.map(x => x.split("\t", -1))
    val tuples = fields
        // remove lines that do not contain the AllNames field
        .filter(x=>x.length > 23)
        // create tuples having as elements the Date and AllNames field for each text line
        .map(x=>(x(1).substring(0,8),x(23)))
    
    // firstly discard the digits pointing to the reference of the name in the article; then separate, filter out blanks and FP and mapReduce. Finally sort and keep 10 highest
    val topics = tuples
        // remove the offsets from the AllNames field
        .map(x => (x._1,x._2.replaceAll("[0-9]","")))
        // separate the Names and create a tuple for each containing the Date also
        .flatMapValues(x=> x.split("[;,]"))
        // remove wrong values
        .filter(x=>x._2!="" && x._2!="ParentCategory" && x._2!="CategoryType" && x._2!="Type ParentCategory")
        // emit 1 for each pair of Date and Name
        .map(x=>((x._1,x._2),1))
        // collect the total counts for each pair
        .reduceByKey(_+_)
        // change the tuple elements' order to use the counts as the key
        .map(_.swap)
        // sort using the counts in descending order
        .sortByKey(false)
        // change the tuple elements' order to its original form
        .map(_.swap)
        // change the key from the (Date, Name) pair to only the Date
        .map(x=> (x._1._1,(x._1._2,x._2)))
        // group using the Date
        .groupByKey()
        // get only the top 10 (Name, counts) pairs
        .map(x=>Array(x._1,x._2.slice(0,10)))
   
    // print each tuple result as strings
    topics.foreach(a => println(a.mkString(" ")))

    // stop Spark's context
    sc.stop()
  }
}
