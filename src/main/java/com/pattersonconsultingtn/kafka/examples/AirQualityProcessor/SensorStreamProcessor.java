package com.pattersonconsultingtn.kafka.examples.PollutionDataTracker;

import java.util.Properties;
import java.util.Random;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;



public class SensorStreamProcessor {

    public static void main(String[] args){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-kafka-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, GenericRecord> source = builder.stream("air_quality_reading");

        // KStream<String, GenericRecord> stats = source.groupByKey()
        //         .aggregate(ReadingStats::new,
        //             (k, v, readingStats) -> GenericData.add(v),
        //             TimeWindows.of(5000).advanceBy(1000),
        //             new GenericAvroSerde(),
        //             "readings-stats-store")
        //         .toStream((key, value) -> new GenericRecord().put(key.key(), key.window().start()))
        //         .mapValues((reading) -> reading.computeAvgPrice());

        stats.to(new GenericAvroSerde(), new GenericAvroSerde(),  "moving-average-output");

        KafkaStreams streams = new KafkaStreams(builder, props);

        streams.cleanUp();

        streams.start();

        Thread.sleep(60000L);

        streams.close();

    }



}
