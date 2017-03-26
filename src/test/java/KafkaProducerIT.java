import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import scala.util.Properties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by Sudipto Saha on 3/26/2017.
 */
public class KafkaProducerIT {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "test";

    @Test
    public void producerTest() throws InterruptedException, IOException {

        // setup Zookeeper
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 3000000, 30000000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        java.util.Properties brokerProps = new java.util.Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new java.util.Properties(), RackAwareMode.Disabled$.MODULE$);

        // setup producer
        java.util.Properties producerProps = new java.util.Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<Integer, byte[]>(producerProps);

        // setup consumer
        java.util.Properties consumerProps = new java.util.Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));

        // send message
        ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC, 42, "test-message".getBytes(StandardCharsets.UTF_8));
        producer.send(data);
        producer.flush();
        producer.close();

        // starting consumer
        ConsumerRecords<Integer, byte[]> records = consumer.poll(3000);
        //assertEquals(1, records.count());
        try {
            Iterator<ConsumerRecord<Integer, byte[]>> recordIterator = records.iterator();
            ConsumerRecord<Integer, byte[]> record = recordIterator.next();
            System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            assertEquals(42, (int) record.key());
            assertEquals("test-message", new String(record.value(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("exception occured");
        }

        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
}
