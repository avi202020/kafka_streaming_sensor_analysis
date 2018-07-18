package com.pattersonconsultingtn.kafka.examples.PollutionDataTracker;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**

Quick Start

# Start Zookeeper. Since this is a long-running service, you should run it in its own terminal.
$ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

# Start Kafka, also in its own terminal.
$ ./bin/kafka-server-start ./etc/kafka/server.properties

# Start the Schema Registry, also in its own terminal.
./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties



// Create topic in Kafka

./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 \
                   --partitions 1 --topic pollution_sensor_reading


// Run the producer from maven

mvn exec:java -Dexec.mainClass="com.pattersonconsultingtn.kafka.examples.PollutionDataTracker.PollutionSensorProducer" \
  -Dexec.args="10 http://localhost:8081"



// check topic for entries
./bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic pollution_sensor_reading --from-beginning

Using the ConsoleConsumer with old consumer is deprecated and will be removed in a future major release. Consider using the new consumer by passing [bootstrap-server] instead of [zookeeper].
{"timestamp":1531332977183,"sensor_reading":0.0}
{"timestamp":1531332978318,"sensor_reading":1.0}
{"timestamp":1531332978318,"sensor_reading":2.0}
{"timestamp":1531332978319,"sensor_reading":3.0}
{"timestamp":1531332978319,"sensor_reading":4.0}
{"timestamp":1531332978319,"sensor_reading":5.0}
{"timestamp":1531332978319,"sensor_reading":6.0}
{"timestamp":1531332978319,"sensor_reading":7.0}
{"timestamp":1531332978320,"sensor_reading":8.0}
{"timestamp":1531332978320,"sensor_reading":9.0}
{"timestamp":1531333171407,"sensor_reading":0.0}
{"timestamp":1531333171816,"sensor_reading":1.0}
{"timestamp":1531333171816,"sensor_reading":2.0}
{"timestamp":1531333171816,"sensor_reading":3.0}
{"timestamp":1531333171817,"sensor_reading":4.0}
{"timestamp":1531333171817,"sensor_reading":5.0}
{"timestamp":1531333171817,"sensor_reading":6.0}
{"timestamp":1531333171817,"sensor_reading":7.0}
{"timestamp":1531333171817,"sensor_reading":8.0}
{"timestamp":1531333171817,"sensor_reading":9.0}


*/
public class PollutionSensorProducer {

  public static void main(String[] args){
    if (args.length != 2) {
      System.out.println("Please provide command line arguments: numEvents schemaRegistryUrl");
      System.exit(-1);
    }
    long events = Long.parseLong(args[0]);
    String url = args[1];

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
    props.put("schema.registry.url", url);

/*

{
  "namespace" : "com.pattersonconsulting.kafka.avro",
  "type" : "record",
  "name" : "Sensor",
  "fields" : [
    {"name":"timestamp","type":"long"},
    {"name":"sensor_reading", "type":"float", "default":0.0} 
  ]
}


*/

    String topicName = "pollution_sensor_reading";
    String topicKey = "sensor_0"; // sensorID

    String schemaString = "{\"namespace\": \"example.avro\", " +
                            "\"type\": \"record\", " +
                           "\"name\": \"" + topicName + "\"," +
                           "\"fields\": [" +
                            "{\"name\": \"timestamp\", \"type\": \"long\"}," +
                            "{\"name\": \"sensor_reading\", \"type\": \"float\", \"default\":0.0 }" +
                           "]}";

    Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse( schemaString );

    Random rnd = new Random();

    for (long nEvents = 0; nEvents < events; nEvents++) {

      long runtime = new Date().getTime();
      float readingVal = nEvents * 1.0f;

      GenericRecord reading_record = new GenericData.Record(schema);
      reading_record.put("timestamp", runtime);
      reading_record.put("sensor_reading", readingVal);
      //page_visit.put("ip", ip);

      // ##############                                                                      topic, key, value
      ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>( topicName, topicKey, reading_record );
      producer.send(data);
    }

    producer.close();
  }
}