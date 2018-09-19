package DF

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp

object dataDF{
    case class GDeltData(
        GKGRECORDID: String,
        DATE: Timestamp,
        SourceCollectionIdentifier: Integer,
        SourceCommonName: String,
        DocumentIdentifier: String,
        Counts: String,
        V2Counts: String,
        Themes: String,
        V2Themes: String,
        Locations: String,
        V2Locations: String,
        Persons: String,
        V2Persons: String,
        Organizations: String,
        V2Organizations: String,
        V2Tone: String,
        Dates: String,
        GCAM: String,
        SharingImage: String,
        RelatedImages: String,
        SocialImageEmbeds: String,
        SocialVideoEmbeds: String,
        Quotations: String,
        AllNames: String,
        Amounts: String,
        TranslationInfo: String,
        Extras: String
    )
    
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
            .appName("Lab 1 - DF")
            .config("spark.master", "local")
            .getOrCreate()
        val sc = spark.sparkContext
        
        import spark.implicits._
        
        val ds = spark.read
                        .schema(schema)
                        .option("sep", "\t")
                        .option("timestampFormat", "yyyyMMddHHmmss")
                        .csv("/home/giannislelekas/Desktop/TU/Supercomputing for Big Data - ET4310/Labs/Lab1/SBD-2018/data/segment/*.csv")
                        //.as[GDeltData]
        
        // Map to the AllNames field for the references within each news feed. Filter out blanks
        val names = ds.map(x => x.getAs[String]("AllNames")).filter(x=> x != "")
        
        // Firstly discard the digits pointing to the spot of the reference within the feed. Then names are extracted, where FP and blanks are filtered out and then mapped to a tuple (String, Int) each. Finally, the counts are measured and the top-10 most referenced names are returned.
        names.map(x=>x.replaceAll("[0-9]", "")).flatMap(x=>x.split("[;,]")).filter(x=>x != "" && x!="ParentCategory" && x!="CategoryType" && x!="Type ParentCategory" ).map(x=>(x,1)).groupBy("_1").sum("_2").sort(desc("sum(_2)")).take(10).foreach(println)
        
                        
        spark.stop
    }
}