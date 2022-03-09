package group.bison.dynamodb.bucket.test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import group.bison.dynamodb.bucket.api.BucketApi;
import group.bison.dynamodb.bucket.simple.SimpleBucket;
import group.bison.dynamodb.bucket.simple.annotation.BucketIdField;
import group.bison.dynamodb.bucket.simple.annotation.BucketIndexField;
import group.bison.dynamodb.bucket.simple.annotation.ItemTimestampField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class BucketTest {

    public static void main(String[] args) {
        AWSCredentials awsCredentials = new BasicAWSCredentials(System.getenv("awsAccessKey"), System.getenv("awsSecretKey"));
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(System.getenv("awsRegion"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        BucketApi<VideoLibrary> bucket = new SimpleBucket<VideoLibrary>("video_library", VideoLibrary.class, dynamoDB);

        VideoLibrary videoLibrary = VideoLibrary.builder().traceId(UUID.randomUUID().toString()).userId(1).timestamp(Long.valueOf(System.currentTimeMillis() / 1000).intValue()).serialNumber("sn_01").deleted(0).imageUrl("http://any.png").ttlTimestamp(System.currentTimeMillis() / 1000 + 600).build();

        String bizId = bucket.add(videoLibrary);
        log.info("bizId {}", bizId);

        VideoLibrary queryVideoLibrary = bucket.queryOne(bizId, videoLibrary.getTraceId(), videoLibrary.getUserId());
        log.info("queryVideoLibraryDO bizId{} imageUrl {}", queryVideoLibrary.getBizId(), queryVideoLibrary.getImageUrl());

        queryVideoLibrary.setImageUrl("http://img2.png");
        queryVideoLibrary.setDeleted(1);
        queryVideoLibrary.setSerialNumber("sn_02");
        bucket.update(queryVideoLibrary.getBizId(), queryVideoLibrary);
        queryVideoLibrary = bucket.queryOne(queryVideoLibrary.getBizId(), queryVideoLibrary.getTraceId(), queryVideoLibrary.getUserId());
        log.info("queryVideoLibraryDO bizId{} imageUrl {}", queryVideoLibrary.getBizId(), queryVideoLibrary.getImageUrl());

        Map<String, String> expressionMap = new HashMap<>();
        Map<String, AttributeValue> expressionValueMap = new HashMap<>();
        expressionMap.put("user_id", "user_id=:userId");
        expressionMap.put("serial_number", "serial_number=:serialNumber");
        expressionValueMap.put(":userId", new AttributeValue().withN("1"));
        expressionValueMap.put(":serialNumber", new AttributeValue().withS("sn_01"));

        List<VideoLibrary> queryVideoLibraryList = bucket.query(expressionMap, Collections.emptyMap(), expressionValueMap, 0, 1000, null);
        log.info("queryVideoLibraryDOList {}", queryVideoLibraryList);

        queryVideoLibraryList.forEach(videoLibrary1 -> bucket.delete(videoLibrary1.getBizId(), videoLibrary1.getTraceId(), videoLibrary1.getUserId()));

        expressionValueMap.put(":serialNumber", new AttributeValue().withS("sn_02"));
        queryVideoLibraryList = bucket.query(expressionMap, Collections.emptyMap(), expressionValueMap, 0, 1000, null);
        log.info("queryVideoLibraryDOList {}", queryVideoLibraryList);
        queryVideoLibraryList.forEach(videoLibrary1 -> bucket.delete(videoLibrary1.getBizId(), videoLibrary1.getTraceId(), videoLibrary1.getUserId()));

        queryVideoLibrary = bucket.queryOne(bizId, videoLibrary.getTraceId(), videoLibrary.getUserId());
        log.info("queryVideoLibraryDO {}", queryVideoLibrary);
    }

    @DynamoDBTable(tableName = "video_library")
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VideoLibrary {

        @DynamoDBHashKey(attributeName = "trace_id")
        private String traceId;

        @BucketIdField
        @DynamoDBRangeKey(attributeName = "user_id")
        private Integer userId;

        @DynamoDBAttribute(attributeName = "id")
        private Integer id;

        @ItemTimestampField
        @DynamoDBAttribute(attributeName = "timestamp")
        private Integer timestamp;

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "serial_number")
        private String serialNumber;

        @DynamoDBAttribute(attributeName = "image_url")
        private String imageUrl;
        @DynamoDBAttribute(attributeName = "deleted")
        private Integer deleted;

        @DynamoDBAttribute(attributeName = "ttl_timestamp")
        private Long ttlTimestamp;

        private String bizId;
    }
}
