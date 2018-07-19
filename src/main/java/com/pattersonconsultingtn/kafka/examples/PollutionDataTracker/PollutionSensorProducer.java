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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

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
 */

public class PollutionSensorProducer {
    
    private static final String CSV_FILE_PATH = "/PattersonConsulting/kafka_examples/AirQualityUCI.csv";
    
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
        
        
        String topicName = "air_quality_reading";
        String topicKey = "sensor_0"; // sensorID
        
        String schemaString = "{\"namespace\": \"example.avro\", " +
        "\"type\": \"record\", " +
        "\"name\": \"" + topicName + "\"," +
        "\"fields\": [" +
        "{\"name\": \"date\", \"type\": \"string\"}," +
        "{\"name\": \"time\", \"type\": \"string\"}," +
        "{\"name\": \"PT08S1\", \"type\": \"string\"}," +
        "{\"name\": \"NMHC\", \"type\": \"string\"}," +
        "{\"name\": \"PT08S2\", \"type\": \"string\"}," +
        "{\"name\": \"NOx\", \"type\": \"string\"}," +
        "{\"name\": \"PT08S3\", \"type\": \"string\"}," +
        "{\"name\": \"NO2\", \"type\": \"string\"}," +
        "{\"name\": \"PT08S4\", \"type\": \"string\"}," +
        "{\"name\": \"PT08S5\", \"type\": \"string\"}" +
        "]}";
        
        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);
        
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse( schemaString );
        Reader reader = null;
        CSVParser csvParser = null;
        
        try {
            reader = Files.newBufferedReader(Paths.get(CSV_FILE_PATH));
            csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                                      .withFirstRecordAsHeader());
            for (CSVRecord csvRecord : csvParser) {
                String date = csvRecord.get("Date");
                String time = csvRecord.get("Time");
                String pts1 = csvRecord.get("PT08.S1(CO)");
                String nmhc = csvRecord.get("NMHC(GT)");
                String pts2 = csvRecord.get("PT08.S2(NMHC)");
                String nogt = csvRecord.get("NOx(GT)");
                String pts3 = csvRecord.get("PT08.S3(NOx)");
                String no2gt = csvRecord.get("NO2(GT)");
                String pts4 = csvRecord.get("PT08.S4(NO2)");
                String pts5 = csvRecord.get("PT08.S5(O3)");
                
                
                GenericRecord reading_record = new GenericData.Record(schema);
                reading_record.put("date", date);
                reading_record.put("time", time);
                reading_record.put("PT08S1", pts1);
                reading_record.put("NMHC", nmhc);
                reading_record.put("PT08S2", pts2);
                reading_record.put("NOx", nogt);
                reading_record.put("PT08S3", pts3);
                reading_record.put("NO2", no2gt);
                reading_record.put("PT08S4", pts4);
                reading_record.put("PT08S5", pts5);
                ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>( topicName, topicKey, reading_record );
                producer.send(data);
            }
        }catch(Exception e) {
            System.out.println(e);
        }
        producer.close();
    }
}

