package com.pattersonconsultingtn.kafka.examples.AirQualityProcessor;

import java.util.Properties;
import java.util.Random;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;




public class SensorStreamProcessor {

  static public final class AverageReadingSerde extends WrapperSerde<SensorAggregator> {

    public AverageReadingSerde() {
        super(new JsonSerializer<SensorAggregator>(), new JsonDeserializer<SensorAggregator>(SensorAggregator.class));
    }
  }

    public static void main(String[] args){

      //Kafka Stream Props
      Properties props = new Properties();
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-processor");
      props.put(StreamsConfig.CLIENT_ID_CONFIG, "processor-client");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
      // Where to find the Confluent schema registry instance(s)
      props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
      // Specify default (de)serializers for record keys and for record values.
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

      final StreamsBuilder builder = new StreamsBuilder();

      KStream<String, GenericRecord> source = builder.stream("PT08S1");

      KGroupedStream<String, GenericRecord> groupedSensorReading = source.groupByKey();

      KStream<String, SensorAggregator>  windowedAverageStream = groupedSensorReading.aggregate(
              SensorAggregator::new,
              (k, v, sensorAggregator) -> sensorAggregator.add(v),
              TimeWindows.of(500),
              new AverageReadingSerde(),
              "temp-store")
              .toStream((key, value) -> value.sensorType)
              .mapValues((reading) -> reading.computeAvgPrice());

      KStream<String, SensorAggregator> printStream = windowedAverageStream.peek(
          new ForeachAction<String, SensorAggregator>() {
            @Override
            public void apply(String key, SensorAggregator value) {
              System.out.println("Sensor=" + key + ", Average=" + value.avg);
            }
          });

        // tempTable.to(Serdes.String(), double or AverageReading);

       final KafkaStreams streams = new KafkaStreams(builder.build(), props);

       final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("sensor-anon-processor-shutdown-hook") {
             @Override
             public void run() {
                 streams.close();
                 latch.countDown();
             }
         });
         try {
           System.out.println("Starting............");
             streams.start();
             latch.await();
         } catch (Throwable e) {
             System.exit(1);
         }
         System.exit(0);
     }

}
