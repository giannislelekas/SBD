package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.JavaConversions._


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
  
  // Create an in-memory state store using (Name as key, count as value)
  val  countStoreSupplier: StoreBuilder[KeyValueStore[String, Long]] =
    Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore("hist"),
      Serdes.String,
      Serdes.Long)
  
  // Add it to the stream context
  builder.addStateStore(countStoreSupplier)


  // Pass each record of the stream thourgh a transformer, along with the state store
  val namesCounts: KStream[String,Long] = allNames.transform(new HistogramTransformer, "hist")

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
  var kvStore: KeyValueStore[String, Long] = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    //Maybe initialize the State Store here, follows logic of GDELTConsumer.scala 
    this.kvStore = this.context.getStateStore("hist").asInstanceOf[KeyValueStore[String, Long]]
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    // Example of taking the event-time timestamp created by Kafka for each stream record
    val timestamp = this.context.timestamp()
    println(timestamp)
    val existing = Option(this.kvStore.get(name))
    var count = 1.toLong

    // Check if name exists already in the state store
    if (existing == None) {
      // Put it in the state store with count = 1 if it appears for the first time
      this.kvStore.put(name, count)
    }else{
      // Update count for the name on the store
      count = this.kvStore.get(name) + 1.toLong
      this.kvStore.put(name, count)
    }
    // Pass the new record to the stream, it is used by GDELT consumer to update the histogram
    return (name, count)
  }

  // Close any resources if any
  def close() {
  }
}
