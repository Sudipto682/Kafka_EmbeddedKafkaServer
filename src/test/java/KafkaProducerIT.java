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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.*;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import scala.util.Properties;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
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
    private int port;
    private  Buffer<KafkaServer> servers;
    private java.util.Properties producerProps;
    private java.util.Properties consumerProps;
    private KafkaServer kafkaServer;
    private ZkClient zkClient;
    private EmbeddedZookeeper zkServer;

    @Before
    public void kafkaSetUp() throws InterruptedException, IOException {

        //find any free port
        port=findPort();

        // setup Zookeeper
        EmbeddedZookeeper zkServer=new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        System.out.println("zookeeper running on port:"+zkServer.port());
        System.out.println("zookeeper directory:"+zkServer.logDir());
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        java.util.Properties brokerProps = new java.util.Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + port);
        KafkaConfig config = new KafkaConfig(brokerProps);
        kafka.utils.Time mock=new kafka.utils.MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config,mock);
        servers= JavaConversions.asScalaBuffer(Arrays.asList(kafkaServer));
        System.out.println(kafkaServer.kafkaHealthcheck());
        System.out.println("kafka server running on port"+":"+kafkaServer.config().port());
        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC, 2, 1, new java.util.Properties(), RackAwareMode.Disabled$.MODULE$);
        TestUtils.waitUntilMetadataIsPropagated(servers,TOPIC,0,50000);
        TestUtils.waitUntilLeaderIsKnown(servers,TOPIC,0,5000);

        //getting partitions of the topic
        /*TopicPartition p0 = new TopicPartition( TOPIC, 0);
        TopicPartition p1 = new TopicPartition(TOPIC, 1);
        TopicPartition p2 = new TopicPartition(TOPIC, 2);
*/
        // setup producer
        java.util.Properties producerProps = new java.util.Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + port);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // setup consumer
        java.util.Properties consumerProps = new java.util.Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + port);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        //consumer.assign(Arrays.asList(p0,p1));
        // send message
        ProducerRecord<Integer, String> data = new ProducerRecord<>(TOPIC, 42, "test-message");
        producer.send(data);
        producer.flush();
        producer.close();

        // starting consumer
        ConsumerRecords<Integer, String> records = consumer.poll(300000);
        assertEquals(1, records.count());
        try {
            Iterator<ConsumerRecord<Integer, String>> recordIterator = records.iterator();
            ConsumerRecord<Integer, String> record = recordIterator.next();
            System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value().toString());
            assertEquals(42, (int) record.key());
            assertEquals("test-message", new String(record.value()));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("exception occured");
        }



    }

    private int findPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
    @Test
    public void kafkaTest()
    {

    }

    @After
    public void finish()
    {
       // kafkaServer.shutdown();
        //zkClient.close();
        //zkServer.shutdown();
    }
}
