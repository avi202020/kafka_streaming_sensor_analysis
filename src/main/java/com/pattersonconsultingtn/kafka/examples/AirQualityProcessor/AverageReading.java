package com.pattersonconsultingtn.kafka.examples.AirQualityProcessor;

import org.apache.avro.generic.GenericRecord;


public class AverageReading {
    int count;
    double avg;
    double sum;
    String sensorType;

    public AverageReading add(GenericRecord record) {
        if(this.sensorType == null){
          this.sensorType = record.get("sensor").toString();
        }
        this.count = this.count + 1;
        this.sum = this.sum + new Double(record.get("reading").toString());
        return this;
    }

    public AverageReading computeAvgPrice(){
       this.avg = this.sum/this.count;
      return this;
    }

}
