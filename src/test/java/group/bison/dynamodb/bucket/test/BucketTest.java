package group.bison.dynamodb.bucket.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
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
import group.bison.dynamodb.bucket.api.BucketApi;
import group.bison.dynamodb.bucket.common.domain.DataQueryParam;
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
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;

@Slf4j
public class BucketTest {

    public static void main(String[] args) {
        ((Logger) LoggerFactory.getILoggerFactory().getLogger("ROOT")).setLevel(Level.INFO);

        AWSCredentials awsCredentials = new BasicAWSCredentials(System.getenv("awsAccessKey"), System.getenv("awsSecretKey"));
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(System.getenv("awsRegion"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
        deleteItemRequest.setTableName("bucket-video_library");
        deleteItemRequest.setKey(new HashMap<String, AttributeValue>() {{
            put(KEY_BUCKET_ID, new AttributeValue().withS("1"));
            put(KEY_START_BUCKET_WINDOW, new AttributeValue().withN(String.valueOf(System.currentTimeMillis() / 60 / 60 / 1000)));
        }});
        dynamoDB.deleteItem(deleteItemRequest);

        BucketApi<VideoLibrary> bucket = new SimpleBucket<VideoLibrary>("video_library", VideoLibrary.class, dynamoDB, null);

        VideoLibrary videoLibrary = VideoLibrary.builder().traceId(UUID.randomUUID().toString()).userId(1).timestamp(Long.valueOf(System.currentTimeMillis() / 1000).intValue()).serialNumber("sn_01")
                .tags(new HashSet<String>() {{
                    add("PERSON");
                }})
                .deleted(0).imageUrl("http://any.png").ttlTimestamp(System.currentTimeMillis() / 1000 + 600).build();

        String bizId = bucket.add(videoLibrary);
        log.info("bizId {}", bizId);

        VideoLibrary queryVideoLibrary = bucket.queryOne(bizId, videoLibrary.getTraceId(), videoLibrary.getUserId());
        log.info("queryVideoLibraryDO bizId{} imageUrl {}", queryVideoLibrary.getBizId(), queryVideoLibrary.getImageUrl());

        queryVideoLibrary.setImageUrl("http://img2.png");
        queryVideoLibrary.setDeleted(null);
        queryVideoLibrary.setSerialNumber("sn_02");
        queryVideoLibrary.getTags().add("VEHICLE");
        queryVideoLibrary.setShareUserIds(Arrays.asList(1, 2, 3));
        bucket.update(queryVideoLibrary.getBizId(), queryVideoLibrary);
        queryVideoLibrary = bucket.queryOne(queryVideoLibrary.getBizId(), queryVideoLibrary.getTraceId(), queryVideoLibrary.getUserId());
        log.info("queryVideoLibraryDO bizId{} imageUrl {} deleted {} shareUserIds {}", queryVideoLibrary.getBizId(), queryVideoLibrary.getImageUrl(), queryVideoLibrary.getDeleted(), queryVideoLibrary.getShareUserIds());

        Map<String, String> expressionMap = new HashMap<>();
        Map<String, AttributeValue> expressionValueMap = new HashMap<>();

        expressionMap.put("user_id", "user_id=:userId");
        expressionMap.put("image_url", "image_url=:image_url");
        expressionMap.put("share_user_ids", "size(share_user_ids) > :zero");
        expressionValueMap.put(":userId", new AttributeValue().withN(String.valueOf(videoLibrary.getUserId())));
        expressionValueMap.put(":image_url", new AttributeValue().withS("http://img2.png"));
        expressionValueMap.put(":zero", new AttributeValue().withN("0"));

        DataQueryParam dataQueryParam = DataQueryParam.builder().expressionMap(expressionMap).expressionNameMap(Collections.emptyMap()).expressionValueMap(expressionValueMap).from(0).to(1000).build();
        List<VideoLibrary> queryVideoLibraryList1 = bucket.query(dataQueryParam, null);
        log.info("queryVideoLibraryDOList {}", queryVideoLibraryList1);

        expressionMap.put("serial_number", "serial_number=:serialNumber");
        expressionMap.put("tags", "contains(tags, :tag)");
        expressionValueMap.put(":serialNumber", new AttributeValue().withS("sn_02"));
        expressionValueMap.put(":tag", new AttributeValue().withS("VEHICLE"));
        List<VideoLibrary> queryVideoLibraryList2 = bucket.query(dataQueryParam, null);
        log.info("queryVideoLibraryDOList {}", queryVideoLibraryList2);

        queryVideoLibraryList2.forEach(videoLibrary1 -> bucket.delete(videoLibrary1.getBizId(), videoLibrary1.getTraceId(), videoLibrary1.getUserId()));

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
        @DynamoDBAttribute(attributeName = "tags")
        private Set<String> tags;

        @DynamoDBAttribute(attributeName = "image_url")
        private String imageUrl;
        @DynamoDBAttribute(attributeName = "deleted")
        private Integer deleted;

        @DynamoDBAttribute(attributeName = "ttl_timestamp")
        private Long ttlTimestamp;

        private String bizId;
    }
}
