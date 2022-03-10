package group.bison.dynamodb.bucket.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.amazon.dax.client.dynamodbv2.AmazonDaxClientBuilder;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import group.bison.dynamodb.bucket.api.BucketApi;
import group.bison.dynamodb.bucket.simple.SimpleBucket;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BucketBenchmarkTest {

    public static void main(String[] args) throws InterruptedException {
        ((Logger) LoggerFactory.getILoggerFactory().getLogger("ROOT")).setLevel(Level.INFO);

        AWSCredentials awsCredentials = new BasicAWSCredentials(System.getenv("awsAccessKey"), System.getenv("awsSecretKey"));
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(System.getenv("awsRegion"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        AmazonDynamoDB daxDynamoDB = AmazonDaxClientBuilder.standard()
                .withRegion(System.getenv("awsRegion"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withEndpointConfiguration(System.getenv("daxEndpoint"))
                .build();

        try {
//            dynamoDB.deleteTable("bucket-video_library");
        } catch (Exception e) {
        }

        BucketApi<BucketTest.VideoLibrary> bucket = new SimpleBucket<BucketTest.VideoLibrary>("video_library", BucketTest.VideoLibrary.class, dynamoDB, daxDynamoDB);

//        Thread.sleep(5000);
        AtomicInteger j = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            CompletableFuture.runAsync(() -> {
                while (j.incrementAndGet() < 100000) {
                    try {
                        BucketTest.VideoLibrary videoLibrary = BucketTest.VideoLibrary.builder().traceId(UUID.randomUUID().toString()).userId(1).timestamp(Long.valueOf(System.currentTimeMillis() / 1000).intValue()).serialNumber("sn_01")
                                .tags(new HashSet<String>() {{
                                    add("PERSON");
                                }})
                                .deleted(0).imageUrl("http://any.png").ttlTimestamp(System.currentTimeMillis() / 1000 + 600).build();
                        bucket.add(videoLibrary);
                    } catch (Exception e) {
                    } finally {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }
}
