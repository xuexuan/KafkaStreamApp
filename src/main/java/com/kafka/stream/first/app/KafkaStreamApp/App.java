package com.kafka.stream.first.app.KafkaStreamApp;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main11( String[] args )
    {
    	Properties config = new Properties();
    	config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
    	config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    	config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    	config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	
    	KStreamBuilder builder = new KStreamBuilder();
    	KStream<String, String> wordcountinput = builder.stream("word-count-input");
    	KTable<String, Long> wordscount = wordcountinput
    			.mapValues(textline->textline.toLowerCase())
    			.flatMapValues(lowercasetext->Arrays.asList(lowercasetext.split(" ")))
    			.selectKey((ignorkey, word) -> word)
    			.groupByKey()
    			.count("Counts");
    	wordscount.to(Serdes.String(), Serdes.Long(), "word-count-output");
    	KafkaStreams stream = new KafkaStreams(builder, config);
    	stream.start();
    	System.out.println(stream.toString());
    	Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
    
  
}
