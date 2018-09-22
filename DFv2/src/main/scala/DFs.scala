package DFs

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp
import org.apache.log4j.{Level, Logger}

object dataDFs{
    
    def main(args: Array[String]) {
      // create the schema of the GDelt GKG data
        val schema = 
            StructType(
               Array(
                    StructField("GKGRECORDID", StringType, nullable=true),
                    StructField("DATE", TimestampType, nullable=true),
                    StructField("SourceCollectionIdentifier", IntegerType, nullable=true),
                    StructField("SourceCommonName", StringType, nullable=true),
                    StructField("DocumentIdentifier", StringType, nullable=true),
                    StructField("Counts", StringType, nullable=true),
                    StructField("V2Counts", StringType, nullable=true),
                    StructField("Themes", StringType, nullable=true),
                    StructField("V2Themes", StringType, nullable=true),
                    StructField("Locations", StringType, nullable=true),
                    StructField("V2Locations", StringType, nullable=true),
                    StructField("Persons", StringType, nullable=true),
                    StructField("V2Persons", StringType, nullable=true),
                    StructField("Organizations", StringType, nullable=true),
                    StructField("V2Organizations", StringType, nullable=true),
                    StructField("V2Tone", StringType, nullable=true),
                    StructField("Dates", StringType, nullable=true), 
                    StructField("GCAM", StringType, nullable=true),
                    StructField("SharingImage", StringType, nullable=true),
                    StructField("RelatedImages", StringType, nullable=true),
                    StructField("SocialImageEmbeds", StringType, nullable=true),
                    StructField("SocialVideoEmbeds", StringType, nullable=true),
                    StructField("Quotations", StringType, nullable=true),
                    StructField("AllNames", StringType, nullable=true),
                    StructField("Amounts", StringType, nullable=true),
                    StructField("TranslationInfo", StringType, nullable=true),
                    StructField("Extras", StringType, nullable=true)
                )
            )
            
        // create a SparakSession needed for using the DataFrames and DataSets APIs
        val spark = SparkSession
            .builder
            .appName("Lab 1 - DFs")
            .config("spark.master", "local")
            .getOrCreate()
        
        import spark.implicits._
        
        // Read the input data, separate the fields and store them in a DataSet
        val ds = spark.read
            .schema(schema)
            .option("sep", "\t")
            .option("timestampFormat", "yyyyMMddHHmmss")
            .csv("../*.csv")            
        
        val df = ds
            // filter out rows with empty AllNames columns
            .filter(x => x.getAs[String]("AllNames") != null)
            // create tuples of the (Date, AllNames) form
            .map(x => (x.getAs[Timestamp]("DATE").toString().substring(0,10), x.getAs[String]("AllNames")))
            // remove the offsets using a regular expression that captures only digits
            .withColumn("_2", regexp_replace(col("_2"), "[0-9]", ""))
            // separate Names
            .map(x => (x.getAs[String]("_1"), x.getAs[String]("_2").split("[;,]")))
            // flatten each AllNames to a single record per Name
            .withColumn("_2", explode($"_2"))
            // remove wrong values
            .filter(x => x.getAs[String]("_2")!= "" && x.getAs[String]("_2")!="Type ParentCategory")
            // emit 1 for each (Date, Name) pair
            .map(x => (x.getAs[String]("_1"), x.getAs[String]("_2"), 1))
            // use (Date, Name) as the key to group all counts for each pair
            .groupBy("_1", "_2")
            // compute the total count for each (Date, Name)
            .agg(sum("_3"))
            // sort in a descending order based on counts
            .sort(desc("sum(_3)"))
        
        // create groupings based on Date, displaying results in a count descending way
        val window = Window.partitionBy("_1").orderBy(desc("sum(_3)"))
        
        //Display results in a  Date, Name, Count, Rank format
        df.select(col("*"), rank().over(window).alias("rank")).where(col("rank")<=10).show()
                                   
        // Stop Spark Session
        spark.stop
    }
}
