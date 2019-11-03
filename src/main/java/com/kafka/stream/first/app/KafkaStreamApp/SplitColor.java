package com.kafka.stream.first.app.KafkaStreamApp;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class SplitColor {

	public static void main(String[] args)
	{
		Properties config = new Properties();
    	config.put(StreamsConfig.APPLICATION_ID_CONFIG, "splitcolor-application");
    	config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    	config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    	config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	
    	KStreamBuilder builder = new KStreamBuilder();
    	KStream<String, String> wordcountinput = builder.stream("color-raw-input");
    	KStream<String, String> usercolormap = wordcountinput
    			.filter((key, value)->value.contains(","))
    			.selectKey((key, value)->value.split(",")[0].toLowerCase())
    			.mapValues(value -> value.split(",")[1].toLowerCase())
    			.filter((user, color)-> Arrays.asList("red","green","blue").contains(color));
    	
    	usercolormap.to("user-color-map");
    	KTable<String, String> usercolortable = builder.table("user-color-map");
    	KTable<String, Long> colorcount = usercolortable
    			.groupBy((key, value)->new KeyValue<>(value, value))
    			.count("CountByColor");
    	
    	colorcount.to(Serdes.String(), Serdes.Long(), "color-raw-output");
    	KafkaStreams stream = new KafkaStreams(builder, config);
    	stream.start();
    	System.out.println(stream.toString());
    	Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
	}
}
