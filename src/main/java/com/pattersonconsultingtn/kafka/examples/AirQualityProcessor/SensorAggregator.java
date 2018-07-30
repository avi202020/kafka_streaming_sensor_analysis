package com.pattersonconsultingtn.kafka.examples.AirQualityProcessor;

import org.apache.avro.generic.GenericRecord;


public class SensorAggregator {
    //number of readings per window
    int count;
    //sum of the given window
    double sum;
    //type of sensor data
    String sensorType;
    //avgerage: computed one time per aggregator
    double avg;


    public SensorAggregator add(GenericRecord record) {
        if(this.sensorType == null){
          this.sensorType = record.get("sensor").toString();
        }

        this.count = this.count + 1;
        this.sum = this.sum + new Double(record.get("reading").toString());
        return this;
    }

    public SensorAggregator computeAvgPrice(){
       this.avg = this.sum/this.count;
      return this;
    }

}
