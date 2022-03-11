package group.bison.dynamodb.bucket.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_MAP;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;

@Slf4j
public class DynamodbTest {

    public static void main(String[] args) {
        ((Logger) LoggerFactory.getILoggerFactory().getLogger("ROOT")).setLevel(Level.INFO);

        AWSCredentials awsCredentials = new BasicAWSCredentials(System.getenv("awsAccessKey"), System.getenv("awsSecretKey"));
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(System.getenv("awsRegion"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        PutItemRequest putItemRequest = new PutItemRequest();
        putItemRequest.setTableName("bucket-video_library");
        putItemRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        Map<String, AttributeValue> itemAttributeValueMap = new HashMap<>();
        itemAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS("-1"));
        itemAttributeValueMap.put(KEY_START_BUCKET_WINDOW, new AttributeValue().withN("-1"));
        itemAttributeValueMap.put(KEY_ITEM_MAP, new AttributeValue().withM(new HashMap<String, AttributeValue>() {{
            put("id", new AttributeValue().withN("1"));
            put("id1", new AttributeValue().withN("1"));
            put("id2", new AttributeValue().withN("1"));
            put("id3", new AttributeValue().withN("1"));
            put("id4", new AttributeValue().withN("1"));
            put("id5", new AttributeValue().withN("1"));
            put("id6", new AttributeValue().withN("1"));
            put("id7", new AttributeValue().withN("1"));
            put("id8", new AttributeValue().withN("1"));
            put("idSet", new AttributeValue().withSS("1"));
            put("idMap", new AttributeValue().withM(Collections.emptyMap()));
        }}));

        putItemRequest.setItem(itemAttributeValueMap);

        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        log.info("putItemResult capacity {}", putItemResult.getConsumedCapacity());

        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        updateItemRequest.setTableName("bucket-video_library");
        updateItemRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        itemAttributeValueMap.remove(KEY_ITEM_MAP);
        updateItemRequest.setKey(itemAttributeValueMap);

        updateItemRequest.setUpdateExpression("SET item_map.one = :one,ttl_timestamp=:ttl_timestamp");

        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        attributeValueMap.put(":ttl_timestamp", new AttributeValue().withN(String.valueOf(System.currentTimeMillis())));
        attributeValueMap.put(":one", new AttributeValue().withM(new HashMap<String, AttributeValue>() {{
            put("id", new AttributeValue().withN("1"));
            put("id1", new AttributeValue().withN("1"));
            put("id2", new AttributeValue().withN("1"));
            put("id3", new AttributeValue().withN("1"));
            put("id4", new AttributeValue().withN("1"));
            put("id5", new AttributeValue().withN("1"));
            put("id6", new AttributeValue().withN("1"));
            put("id7", new AttributeValue().withN("1"));
            put("id8", new AttributeValue().withN("1"));
            put("idSet", new AttributeValue().withSS("1"));
            put("idMap", new AttributeValue().withM(Collections.emptyMap()));
        }}));
        updateItemRequest.setExpressionAttributeValues(attributeValueMap);

        UpdateItemResult updateItemResult = dynamoDB.updateItem(updateItemRequest);
        log.info("updateItemResult capacity {}", updateItemResult.getConsumedCapacity());

    }
}
