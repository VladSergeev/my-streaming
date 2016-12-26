package my.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by vsergeev on 10.06.2016.
 */
@Controller
@RequestMapping(value = "/stream")
public class StreamController {

    private static final String kafkaUrls="";
    private static final String zookeeperUrls="";

    @RequestMapping(value = "/createFlatMapKStream", method = RequestMethod.GET)
    @ResponseBody
    public void create() throws Exception {
        createFlatMapKStream();
    }

    @RequestMapping(value = "/createSimpleMapKStream", method = RequestMethod.GET)
    @ResponseBody
    public void map() throws Exception {
        createSimpleMapKStream();
    }


    @RequestMapping(value = "/createOwnProcessorStream", method = RequestMethod.GET)
    @ResponseBody
    public void processor() throws Exception {
        createOwnProcessorStream();
    }


    @RequestMapping(value = "/findMinimum", method = RequestMethod.GET)
    @ResponseBody
    public void minimum() throws Exception {
        findMinimum();
    }



    private void findMinimum() {
        KafkaStreams streams = null;
        try {

            KStreamBuilder builder = new KStreamBuilder();
            Serde<String> stringSerde = Serdes.String();
            Serde<Long> longSerde = Serdes.Long();
            KStream<String, String> kStream = builder.stream(stringSerde, stringSerde, "test_minimum2_input");

            kStream.reduceByKey(new Reducer<String>() {
                @Override
                public String apply(String v1, String v2) {
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
                        String result = "";
                        String[] val1 = v1.split("_");
                        String[] val2 = v2.split("_");
                        Long value1 = Long.valueOf(val1[0]);
                        Long value2 = Long.valueOf(val2[0]);
                        if (value1.compareTo(value2) <= 0) {
                            result = value1.toString() + "_" + val1[1] + "_end:" + sdf.format(new Date());
                        } else {
                            result = value2.toString() + "_" + val2[1] + "_end:" + sdf.format(new Date());
                        }
                        return result;
                    } catch (Exception e) {
                        return "Not valid value v1 = " + v1 + "  v2 = " + v2;
                    }
                }
            }, "reduce-minimum").to("test_minimum2_output");
            streams = new KafkaStreams(builder, kafkaStreamConfig());
            streams.start();
        } catch (Exception e) {
            if (streams != null) {
                streams.close();
            }

        }
    }
  private void createFlatMapKStream() {
        KafkaStreams streams = null;
        try {

            KStreamBuilder builder = new KStreamBuilder();
            Serde<String> stringSerde = Serdes.String();
            Serde<Long> longSerde = Serdes.Long();
            KStream<String, String> kStream = builder.stream("test_input");
            KTable<String, Long> counts = kStream
                    .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                        @Override
                        public Iterable<String> apply(String value) {
                            return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
                        }
                    }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                        @Override
                        public KeyValue<String, String> apply(String key, String value) {
                            return new KeyValue<>(value, value);
                        }
                    })
                    .countByKey(stringSerde, "Counts");

            // need to override value serde to Long type
            counts.to(Serdes.String(), Serdes.Long(), "test_output");
            //mapValues(o -> o+" TEST").to("test_output");
            streams = new KafkaStreams(builder, kafkaStreamConfig());
            streams.start();
        } catch (Exception e) {
            if (streams != null) {
                streams.close();
            }

        }
    }

    private void createSimpleMapKStream() {
        KafkaStreams streams = null;
        try {

            KStreamBuilder builder = new KStreamBuilder();

            KStream<String, String> kStream = builder.stream("test_input");
            kStream.mapValues(o -> o + " TEST").to("test_output");
            streams = new KafkaStreams(builder, kafkaStreamConfig());
            streams.start();
        } catch (Exception e) {
            if (streams != null) {
                streams.close();
            }

        }
    }

    private void createOwnProcessorStream() {
        KafkaStreams streams = null;
        try {
            StateStoreSupplier countStore = Stores.create("Counts")
                    .withKeys(Serdes.String())
                    .withValues(Serdes.String())
                    .inMemory()
                    .build();
            TopologyBuilder builder = new TopologyBuilder();
            builder.addSource("Source", "test_input")
                    .addProcessor("Process", new ProcessorImpl(), "Source")
                    .addStateStore(countStore, "Process")
                    .addSink("Sink", "test_output", "Process");
            streams = new KafkaStreams(builder, kafkaStreamConfig());
            streams.start();
        } catch (Exception e) {
            if (streams != null) {
                streams.close();
            }

        }
    }


    private Properties kafkaStreamConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-kafka-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrls);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperUrls);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
