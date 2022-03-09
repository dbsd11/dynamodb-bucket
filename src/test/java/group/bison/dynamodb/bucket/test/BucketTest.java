package group.bison.dynamodb.bucket.test;

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
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped;
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
import java.util.Set;
import java.util.UUID;

@Slf4j
public class BucketTest {

    public static void main(String[] args) {
        AWSCredentials awsCredentials = new BasicAWSCredentials(System.getenv("awsAccessKey"), System.getenv("awsSecretKey"));
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(System.getenv("awsRegion"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        BucketApi<VideoLibraryDO> bucket = new SimpleBucket<VideoLibraryDO>("video_library", VideoLibraryDO.class, dynamoDB);

        VideoLibraryDO videoLibraryDO = VideoLibraryDO.builder().traceId(UUID.randomUUID().toString()).userId(1).timestamp(Long.valueOf(System.currentTimeMillis() / 1000).intValue()).serialNumber("sn_01").deleted(0).imageUrl("http://any.png").build();

        String bizId = bucket.add(videoLibraryDO);
        log.info("bizId {}", bizId);

        VideoLibraryDO queryVideoLibraryDO = bucket.queryOne(bizId, videoLibraryDO.getTraceId(), videoLibraryDO.getUserId());
        log.info("queryVideoLibraryDO bizId{} imageUrl {}", queryVideoLibraryDO.getBizId(), queryVideoLibraryDO.getImageUrl());

        queryVideoLibraryDO.setImageUrl("http://img2.png");
        queryVideoLibraryDO.setDeleted(1);
        queryVideoLibraryDO.setSerialNumber("sn_02");
        bucket.update(queryVideoLibraryDO.getBizId(), queryVideoLibraryDO);
        queryVideoLibraryDO = bucket.queryOne(queryVideoLibraryDO.getBizId(), queryVideoLibraryDO.getTraceId(), queryVideoLibraryDO.getUserId());
        log.info("queryVideoLibraryDO bizId{} imageUrl {}", queryVideoLibraryDO.getBizId(), queryVideoLibraryDO.getImageUrl());

        Map<String, String> expressionMap = new HashMap<>();
        Map<String, AttributeValue> expressionValueMap = new HashMap<>();
        expressionMap.put("user_id", "user_id=:userId");
        expressionMap.put("serial_number", "serial_number=:serialNumber");
        expressionValueMap.put(":userId", new AttributeValue().withN("1"));
        expressionValueMap.put(":serialNumber", new AttributeValue().withS("sn_01"));

        List<VideoLibraryDO> queryVideoLibraryDOList = bucket.query(expressionMap, Collections.emptyMap(), expressionValueMap, 0, 1000, null);
        log.info("queryVideoLibraryDOList {}", queryVideoLibraryDOList);

        queryVideoLibraryDOList.forEach(videoLibraryDO1 -> bucket.delete(videoLibraryDO1.getBizId(), videoLibraryDO1.getTraceId(), videoLibraryDO1.getUserId()));

        expressionValueMap.put(":serialNumber", new AttributeValue().withS("sn_02"));
        queryVideoLibraryDOList = bucket.query(expressionMap, Collections.emptyMap(), expressionValueMap, 0, 1000, null);
        log.info("queryVideoLibraryDOList {}", queryVideoLibraryDOList);
        queryVideoLibraryDOList.forEach(videoLibraryDO1 -> bucket.delete(videoLibraryDO1.getBizId(), videoLibraryDO1.getTraceId(), videoLibraryDO1.getUserId()));

        queryVideoLibraryDO = bucket.queryOne(bizId, videoLibraryDO.getTraceId(), videoLibraryDO.getUserId());
        log.info("queryVideoLibraryDO {}", queryVideoLibraryDO);
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

        @DynamoDBAttribute(attributeName = "id")
        private Integer id;

        @ItemTimestampField
        @DynamoDBAttribute(attributeName = "timestamp")
        private Integer timestamp;

        @BucketIndexField
        @DynamoDBAttribute(attributeName = "serial_number")
        private String serialNumber;

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

        @DynamoDBAttribute(attributeName = "missing")
        private Integer missing;

        @DynamoDBAttribute(attributeName = "marked")
        private Integer marked;

        @DynamoDBAttribute(attributeName = "admin_id")
        private Integer adminId;

        @DynamoDBAttribute(attributeName = "device_name")
        private String deviceName;

        @DynamoDBAttribute(attributeName = "tags")
        @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.SS)
        private Set<String> tags;

        @DynamoDBAttribute(attributeName = "segment")
        private Integer segment; // 是否是切片

        // 设备端切片，新增加字段
        @DynamoDBAttribute(attributeName = "type")
        private Integer type; // 0-完整视频;1-设备端切片

        @DynamoDBAttribute(attributeName = "received_all_slice")
        private Boolean receivedAllSlice; // 0-切片不完整，1-切片完整

        @DynamoDBAttribute(attributeName = "video_event")
        private String videoEvent;

        @DynamoDBAttribute(attributeName = "ttl_timestamp")
        private Long ttlTimestamp;

        private String bizId;
    }
}
