import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;


/**
 * Created by vsergeev on 07.10.2016.
 */
public class TestSpark {


    private static String kafkaUrls;
    private static String kafkaTopic;
    private static String inputFilePath;
    private static String outputFilePath;
    public static void main(String[] args) throws Exception {
        String method = args[0];


        if ("kafkaProcessing".equals(method)) {
            kafkaUrls = args[1];
            kafkaTopic = args[2];
            kafkaProcessing();
        } else if ("kafkaStreaming".equals(method)) {
            kafkaUrls = args[1];
            kafkaTopic = args[2];
            kafkaStreaming();
        } else if ("kafkaJob".equals(method)) {
            kafkaUrls = args[1];
            kafkaTopic = args[2];
            outputFilePath = args[2];
            kafkaJob();
        } else if ("kafkaTestStreaming".equals(method)) {
            kafkaUrls = args[1];
            kafkaTopic = args[2];
            kafkaTestStreaming();
        } else if ("countWords".equals(method)) {
            inputFilePath = args[1];
            countWords();
        } else if ("wordCount".equals(method)) {
            inputFilePath = args[1];
             outputFilePath = args[2];
            wordCount();
        } else if ("calcPi".equals(method)) {
            calcPi();
        } else {
            throw new Exception("Unknown method!");
        }


    }


    private static void kafkaProcessing() throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaUrls);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "asa");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.offset.reset", "latest");
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList(kafkaTopic),
                        kafkaParams));

        JavaDStream<ConsumerRecord<String, String>> filtered = stream.filter(new Function<ConsumerRecord<String, String>, Boolean>() {
            @Override
            public Boolean call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                return stringStringConsumerRecord.value().contains("job-sm");
            }
        });

        JavaDStream<String> processing = filtered.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                return stringStringConsumerRecord.value().replace("job", "WORKER");
            }
        });


        processing.foreachRDD(stringJavaRDD ->
                stringJavaRDD.foreachPartition(stringIterator -> {
                    Producer<String, String> producer = createProducer(kafkaUrls);
                    stringIterator.forEachRemaining(new Consumer<String>() {
                        @Override
                        public void accept(String s) {

                            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test_topic", "my_key", s);

                            producer.send(record);
                        }
                    });
                }));
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    private static Producer<String, String> createProducer(String url) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, "60000");

        return new KafkaProducer<>(props);
    }

    private static void kafkaStreaming() throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaUrls);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "asa");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.offset.reset", "latest");
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList(kafkaTopic),
                        kafkaParams));

        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                return stringStringConsumerRecord.value();
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(",")).iterator();
            }
        });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

    private static void kafkaTestStreaming() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaUrls);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "asa");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.offset.reset", "latest");
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Collections.singletonList("_METRIC_TOPIC"), kafkaParams));
        String output = "c:/kafka-out";
        stream.filter(new Function<ConsumerRecord<String, String>, Boolean>() {
            public Boolean call(ConsumerRecord<String, String> s) {
                return s.value().contains("job-sm");
            }
        }).print();

        jssc.start();
    }


    private static void kafkaJob() {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaUrls);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "latest");
        kafkaParams.put("enable.auto.commit", "false");
        OffsetRange offsetRange = OffsetRange.create(kafkaTopic, 0, 14813580, 14813610);
        OffsetRange[] arr = new OffsetRange[1];
        arr[0] = offsetRange;
        JavaRDD<ConsumerRecord<String, String>> logData = KafkaUtils.createRDD(sc, kafkaParams, arr, LocationStrategies.PreferConsistent());
        JavaRDD<ConsumerRecord<String, String>> numBs = logData.filter(new Function<ConsumerRecord<String, String>, Boolean>() {
            public Boolean call(ConsumerRecord<String, String> s) {
                return s.value().contains("job-sm");
            }
        });

        numBs.saveAsTextFile(outputFilePath);

        System.out.println("Lines with with good: " + numBs);

    }

    private static void countWords() {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(inputFilePath).cache();
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("dog");
            }
        }).count();
        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("good");
            }
        }).count();
        System.out.println("Lines with dog: " + numAs + ", lines with good: " + numBs);
    }

    private static void wordCount() {
        SparkConf conf = new SparkConf().setAppName("Work Count App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(inputFilePath);
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        counts.saveAsTextFile(outputFilePath);
    }

    private static void calcPi() {

        List<String> args = new ArrayList<String>() {{
            add("test my dog");
            add("dog is good");
            add("ok good");
        }};
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int NUM_SAMPLES = 10000000;
        List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(
                new Function<Integer, Boolean>() {
                    public Boolean call(Integer i) {
                        double x = Math.random();
                        double y = Math.random();
                        return x * x + y * y < 1;
                    }
                }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

    }
}
