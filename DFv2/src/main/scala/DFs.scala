package DFs

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp
import org.apache.log4j.{Level, Logger}

object dataDFs{
    
    def main(args: Array[String]) {
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
            
        val spark = SparkSession
            .builder
            .appName("Lab 1 - DFs")
            .config("spark.master", "local")
            .getOrCreate()
        import spark.implicits._
        
        //val sc = spark.sparkContext
        
        
        val ds = spark.read
                        .schema(schema)
                        .option("sep", "\t")
                        .option("timestampFormat", "yyyyMMddHHmmss")
                        .csv("/home/giannislelekas/Desktop/TU/Supercomputing for Big Data - ET4310/Labs/Lab1/SBD-2018/data/segment/*.csv")
                    
        
        // Map to the AllNames field for the references within each news feed. Filter out blanks
        val df = ds.map(x => (x.getAs[Timestamp]("DATE").toString().substring(0,10),        x.getAs[String]("AllNames")))
        .filter(x => x._2 != null)
        .withColumn("_2", regexp_replace(col("_2"), "[0-9]", ""))
        .map(x => (x.getAs[String]("_1"), x.getAs[String]("_2").split("[;,]")))
        .withColumn("_2", explode($"_2"))
        .filter(x => x.getAs[String]("_2")!= "" && x.getAs[String]("_2")!="Type ParentCategory").map(x => (x.getAs[String]("_1"), x.getAs[String]("_2"), 1))
        .groupBy("_1", "_2").agg(sum("_3")).sort(desc("sum(_3)"))
        
        
        
        val window = Window.partitionBy("_1").orderBy(desc("sum(_3)"))
        df.select(col("*"), rank().over(window).alias("rank")).where(col("rank")<=10).show

        
        
                        
        spark.stop
    }
}