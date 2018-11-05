package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.ArrayList

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.KeyValue

import scala.collection.JavaConversions._
import scala.util.control.Breaks._


object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }


  val builder: StreamsBuilder = new StreamsBuilder
  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally, 
  // write the result to a new topic called gdelt-histogram. 
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  // Transformations similar to lab 1, output is (SegmentTimestamp-articleID as key, Name as value)
  val allNames: KStream[String,String] = records.mapValues(x=> x.split("\t")).filter((k,v) => v.length > 23).mapValues(v => v(23)).mapValues(v => v.split(";")).flatMapValues(v=>v).mapValues(v => v.split(",")).mapValues(v => v(0)).filter((k,v) => v!="" && v!="ParentCategory" && v!="CategoryType" && v!="Type ParentCategory")
  
  // Create an in-memory state store using (topic as key, count as value)
  val  countStoreSupplier: StoreBuilder[KeyValueStore[String, Long]] =
    Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore("hist"),
      Serdes.String,
      Serdes.Long)
  
  // Second state store using (topic as key, string of comma separated timestamps)
  val  timestampStoreSupplier: StoreBuilder[KeyValueStore[String, String]] =
    Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore("timestamps"),
      Serdes.String,
      Serdes.String)

  // Add them to the stream context
  builder.addStateStore(countStoreSupplier)
  builder.addStateStore(timestampStoreSupplier)


  // Pass each record of the stream thourgh a transformer, along with the state store
  val namesCounts: KStream[String,Long] = allNames.transform(new HistogramTransformer, "hist", "timestamps")

  // Create new topic for consumer(both for debugging and input for the visualizer)
  namesCounts.to("gdelt-histogram")

  // All commands must be started before stream.start() in order to have an effect
  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()
  
  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  // Define the state store as a field of the transformer
  var counts: KeyValueStore[String, Long] = _
  var timestamps: KeyValueStore[String, String] = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    
    // Initialize the state stores
    this.counts = this.context.getStateStore("hist").asInstanceOf[KeyValueStore[String, Long]]
    this.timestamps = this.context.getStateStore("timestamps").asInstanceOf[KeyValueStore[ String, String]]

    // Schedule a Punctutator every one minute to find records that are older than an hour
    this.context.schedule(60000, PunctuationType.WALL_CLOCK_TIME, (timestamp) => { 
      // Iterate over the timestamps state store
      val iter: KeyValueIterator[String,String] = this.timestamps.all()
      while (iter.hasNext() ) {
        val entry: KeyValue[String,String] = iter.next()
        val name: String = entry.key
        val nameTimestamps: String = entry.value
        // Create a List of all the timestamps related to a name
        val listNamesTimestamps: List[String] = nameTimestamps.split(',').toList
        // Initialize a List for all timestamps that need to be removed
        val toBeRemoved: ArrayList[String] = new ArrayList
        // Initialize the counter of timestamps that need to be removed
        var c: Int = 0
        breakable{for (i<-0 until listNamesTimestamps.length) {
            if (timestamp - listNamesTimestamps(i).toLong > 60 * 60 * 1000) {
              toBeRemoved.add(listNamesTimestamps(i))
              c += 1  
            }else{
              break
            }
        }}
        if (c != 0){
          // Update the histogram and the consumer 
          this.counts.put(name, this.counts.get(name) - c.toLong)
          this.context.forward(name, this.counts.get(name))
          // Remove the timestamps that were found to correspond to more than an hour later
          this.timestamps.put(name,listNamesTimestamps.diff(toBeRemoved).mkString(","))   
          // If a name has no mentions in the last hour, remove it from the state stores
          if (this.counts.get(name) == 0){
            this.counts.delete(name)
            this.timestamps.delete(name)
          }
        }
      }
      iter.close()
      this.context.commit()
    })
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    // Get the event-time timestamp created by Kafka for each stream record
    val timestamp = this.context.timestamp()
    // Initialize helpers
    val existing = Option(this.counts.get(name))
    var count = 1L
    
    // Put this timestamp into the corresponding state store relating it to the current name
    if (Option(this.timestamps.get(name)) == None){
      this.timestamps.put(name,timestamp.toString)
    }else{
      this.timestamps.put(name,this.timestamps.get(name)+','+timestamp.toString)
    }

    // Check if name exists already in the state store
    if (existing == None) {
      // Put it in the state store with count = 1 if it appears for the first time
      this.counts.put(name, count)
    }else{
      // Update count for the name on the store
      count = this.counts.get(name) + 1L
      this.counts.put(name, count)
    }
    // Pass the new record to the stream, it is used by GDELT consumer to update the histogram
    return (name, count)
  }

  // Close any resources if any
  def close() {
  }
}
