package group.bison.dynamodb.bucket.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.amazon.dax.client.dynamodbv2.AmazonDaxClientBuilder;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverted;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import group.bison.dynamodb.bucket.api.BucketApi;
import group.bison.dynamodb.bucket.simple.SimpleBucket;
import group.bison.dynamodb.bucket.simple.annotation.BucketIdField;
import group.bison.dynamodb.bucket.simple.annotation.BucketIndexField;
import group.bison.dynamodb.bucket.simple.annotation.ItemTimestampField;
import group.bison.dynamodb.bucket.test.util.DynamodbJsonConverter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;

@Slf4j
public class BucketBenchmarkTest {

    public static void main(String[] args) throws InterruptedException {
        ((Logger) LoggerFactory.getILoggerFactory().getLogger("ROOT")).setLevel(Level.INFO);

        AWSCredentials awsCredentials = new BasicAWSCredentials(System.getenv("awsAccessKey"), System.getenv("awsSecretKey"));
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(System.getenv("awsRegion"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

//        AmazonDynamoDB daxDynamoDB = AmazonDaxClientBuilder.standard()
//                .withRegion(System.getenv("awsRegion"))
//                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
//                .withEndpointConfiguration(System.getenv("daxEndpoint"))
//                .build();
        AmazonDynamoDB daxDynamoDB = null;

//        DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
//        deleteItemRequest.setTableName("bucket-video_library");
//        deleteItemRequest.setKey(new HashMap<String, AttributeValue>() {{
//            put(KEY_BUCKET_ID, new AttributeValue().withS("0"));
//            put(KEY_START_BUCKET_WINDOW, new AttributeValue().withN(String.valueOf(System.currentTimeMillis() / 60 / 60 / 1000)));
//        }});
//        dynamoDB.deleteItem(deleteItemRequest);

        BucketApi<VideoLibraryDO> videoLibraryDOBucketApi = new SimpleBucket<>("video_library", VideoLibraryDO.class, dynamoDB, daxDynamoDB);

        ExecutorService executor = Executors.newFixedThreadPool(100);

        Integer userCount = ArrayUtils.getLength(args) > 0 ? Integer.valueOf(args[0]) : 1;

        ConcurrentLinkedQueue userIdQueue = new ConcurrentLinkedQueue();
        for (int i = 0; i < userCount; i++) {
            userIdQueue.add(i);
        }

        CountDownLatch countDownLatch = new CountDownLatch(100000);
        for (int i = 0; i < userCount; i++) {
            CompletableFuture.runAsync(() -> {
                while (true) {
                    Integer userId = null;
                    try {
                        userId = (Integer) userIdQueue.poll();

                        VideoLibraryDO videoLibraryDO = VideoLibraryDO.builder()
                                .userId(userId)
                                .id(Double.valueOf(Math.random() * Integer.MAX_VALUE).intValue())
                                .traceId(UUID.randomUUID().toString())
                                .type(1)
                                .tags(Collections.singleton(""))
                                .adminId(userId)
                                .timestamp(Long.valueOf(System.currentTimeMillis() / 1000).intValue())
                                .serialNumber(Math.random() > 0.8 ? "sn_03" : Math.random() > 0.5 ? "sn_02" : "sn_01")
                                .deviceName("smart camera")
                                .deleted(0).expired(0)
                                .marked(0).missing(1)
                                .imageOnly(0)
                                .shareUserIds(Arrays.asList(userId))
                                .imageUrl("http://aasddasdasddsdasdshdkdhksjhdkdhajdhasadasdasdaksdhadjasldjajdaskdjaldjalskdasdasdd.png")
                                .videoUrl("http://aasddasdasddsdasdshdkdhksjhdkdhajdhasadasdasdadaksldjaldajsldkajdlajdlkasjdlsjlasdjlkajdlkajsdjasdlkjdlkasjd.m3u8")
                                .videoEvent(String.valueOf(System.currentTimeMillis()))
                                .ttlTimestamp(Long.valueOf(System.currentTimeMillis() / 1000 + 8 * 60 * 60))
                                .build();
                        String bizId = videoLibraryDOBucketApi.add(videoLibraryDO);

                        videoLibraryDO.setImageUrl("http://aasddasdasddsdasdshdkdhksjhdkdhajdhasadasdasdaksdhadjasldjajdaskdjaldjalskdasdasdd.png2");
                        videoLibraryDO.setTags(new HashSet<>(Math.random() > 0.5 ? Collections.singleton("PERSON") : Collections.singleton("VEHICLE")));
                        videoLibraryDO.setEventInfo(Collections.singletonList(Collections.singletonMap("obj", "asdjaskdjalsdjlasjdldjalskdjlskdjkljlkj12j3llajsldkjaslkdjasldjasadasdasdsdasddas")));
                        videoLibraryDOBucketApi.update(bizId, videoLibraryDO);

                        videoLibraryDO.setPeriod(10.1123123d);
                        videoLibraryDO.setReceivedAllSlice(true);
                        videoLibraryDOBucketApi.update(bizId, videoLibraryDO);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (userId != null) {
                            userIdQueue.add(userId);
                        }

                        try {
                            Thread.sleep(10);
                            countDownLatch.countDown();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }, executor);
        }

        countDownLatch.await();
    }


    @DynamoDBTable(tableName = "video_library") // 会被配置项${dynamo.videoLibrary.tableName}覆盖
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VideoLibraryDO {

        @DynamoDBHashKey(attributeName = "trace_id")
        private String traceId;

        @BucketIdField
        @DynamoDBRangeKey(attributeName = "user_id")
        private Integer userId;

        @DynamoDBAttribute(attributeName = "share_user_ids")
        @DynamoDBTypeConverted(converter = DynamodbJsonConverter.class)
        @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.L)
        private List shareUserIds;

        @DynamoDBAttribute(attributeName = "id")
        private Integer id;

        @ItemTimestampField
        @DynamoDBAttribute(attributeName = "timestamp")
        private Integer timestamp;

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "serial_number")
        private String serialNumber;

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "activity_zone_id")
        private String activityZoneId;

        @DynamoDBAttribute(attributeName = "image_url")
        private String imageUrl;

        @DynamoDBAttribute(attributeName = "video_url")
        private String videoUrl;

        @DynamoDBAttribute(attributeName = "period")
        private Double period;

        @DynamoDBAttribute(attributeName = "deleted")
        private Integer deleted;

        @DynamoDBAttribute(attributeName = "expired")
        private Integer expired;

        @DynamoDBAttribute(attributeName = "file_size")
        private Integer fileSize;

        @DynamoDBAttribute(attributeName = "image_only")
        private Integer imageOnly;

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "missing")
        private Integer missing;

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "marked")
        private Integer marked;

        @DynamoDBAttribute(attributeName = "admin_id")
        private Integer adminId;

        @DynamoDBAttribute(attributeName = "device_name")
        private String deviceName;

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "tags")
        @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.SS)
        private Set<String> tags;

        @DynamoDBAttribute(attributeName = "event_info")
        @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.L)
        @DynamoDBTypeConverted(converter = DynamodbJsonConverter.class)
        private List eventInfo;

        @DynamoDBAttribute(attributeName = "segment")
        private Integer segment; // 是否是切片

        // 设备端切片，新增加字段
        @DynamoDBAttribute(attributeName = "type")
        private Integer type; // 0-完整视频;1-设备端切片

        @DynamoDBAttribute(attributeName = "received_all_slice")
        private Boolean receivedAllSlice; // 0-切片不完整，1-切片完整

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "video_event")
        private String videoEvent;

        @DynamoDBAttribute(attributeName = "ttl_timestamp")
        private Long ttlTimestamp;
    }
}
