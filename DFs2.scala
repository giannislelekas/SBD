package DFs

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Date


object dataDFs{
    
    def main(args: Array[String]) {
      // create the schema of the GDelt GKG data
        val schema = 
            StructType(
               Array(
                    StructField("GKGRECORDID", StringType, nullable=true),
                    StructField("DATE", DateType, nullable=true),
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
            .appName("DFs")
            //.config("spark.master", "local")
            .getOrCreate()
            //.config(getConfig)
            //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        import spark.implicits._
        
        // Read the input data, separate the fields and store them in a DataSet
        val ds = spark.read
            .schema(schema)
            .option("sep", "\t")
            .option("dateFormat", "yyyyMMddHHmmss") //epistrefei to swsto xwris na allaksw to format
            .csv("/home/giannislelekas/Desktop/TU/Supercomputing for Big Data - ET4310/Labs/Lab1/SBD-2018/data/segment/*.csv")
            //10SEG
            //.csv(Array("s3://gdelt-open-data/v2/gkg/20150218*","s3://gdelt-open-data/v2/gkg/2015021900*","s3://gdelt-open-data/v2/gkg/2015021901[0-1]*") :_*)
            //100SEG
            //.csv(Array("s3://gdelt-open-data/v2/gkg/2015021[8-9]*","s3://gdelt-open-data/v2/gkg/201502200[0-1]*") :_*)
            //1000seg
            //.csv(Array("s3://gdelt-open-data/v2/gkg/201502*","s3://gdelt-open-data/v2/gkg/201503010*","s3://gdelt-open-data/v2/gkg/2015030110*") :_*)
            //10000seg
            //.csv(Array("s3://gdelt-open-data/v2/gkg/20150[2-5]*","s3://gdelt-open-data/v2/gkg/2015060[1-2]*","s3://gdelt-open-data/v2/gkg/201506030[0-6]*","s3://gdelt-open-data/v2/gkg/20150603070000.gkg.csv") :_*)
            //100000seg
            //.csv(Array("s3://gdelt-open-data/v2/gkg/201[5-7]*","s3://gdelt-open-data/v2/gkg/2018010[1-7]*,s3://gdelt-open-data/v2/gkg/20180108[00-17]*") :_*)
            //entire
            .csv("s3://gdelt-open-data/v2/gkg/*.csv")
            
            
        
        
        // ta string imerominies nan ginoun int
        ds.createOrReplaceTempView("articles")
        val df = spark.sql("SELECT DATE,AllNames FROM articles WHERE AllNames IS NOT NULL")
        //val df = ds
            // filter out rows with empty AllNames columns
            //.filter(x => x.getAs("AllNames") != null)
            // create tuples of the (Date, AllNames) form
            //.select("DATE", "AllNames")   **de xreaizetai afou kanoume to map parakatw
            // remove the offsets using a regular expression that captures only digits
            .withColumn("AllNames", regexp_replace(col("AllNames"), "[0-9]", ""))
            // separate Names
            .map(x => (x.getAs[Date]("DATE"), x.getAs[String]("AllNames").split("[;,]")))
            // flatten each AllNames to a single record per Name
            .withColumn("_2", explode($"_2"))
            // remove wrong values
            .filter(x => x.getAs("_2")!= "" && x.getAs("_2")!="Type ParentCategory")
            // emit 1 for each (Date, Name) pair
            //.map(x => (x.getAs[String]("_1"), x.getAs[String]("_2"), 1))
            // use (Date, Name) as the key to group all counts for each pair
            .groupBy("_1", "_2")
            // compute the total count for each (Date, Name)
            .count
            // sort in a descending order based on counts
            //.sort(desc("count"))
        
        // create groupings based on Date, displaying results in a count descending way
        val window = Window.partitionBy("_1").orderBy(desc("count"))
        
        //Display results in a  Date, Name, Count, Rank format
        df.select(col("*"), rank().over(window).alias("rank")).where(col("rank")<=10).show()
        //df.select(col("*"), rank().over(window).alias("rank")).where(col("rank")<=10).select("_1","_2","count").show
                                   
        // Stop Spark Session
        spark.stop
    }
}
