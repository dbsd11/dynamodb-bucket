package group.bison.dynamodb.bucket.metadata;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.http.impl.io.EmptyInputStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_S3_STORAGE_URL;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_COUNT;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_MAP;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;
import static group.bison.dynamodb.bucket.common.Constants.MAX_BUCKET_ITEM_COUNT;
import static group.bison.dynamodb.bucket.common.Constants.NS_EMPTY_VALUE;
import static group.bison.dynamodb.bucket.common.Constants.SS_EMPTY_STR;

@Slf4j
@NoArgsConstructor
public class BucketMetaDataMapper {

    private String bucketTableName;

    private AmazonDynamoDB dynamoDB;

    private Optional<AmazonS3> amazonS3Optional;

    private static Set<String> bucketIndexSet = new HashSet<>();

    private static Map<String, Object> currentBucketWindowMap = new HashMap<>();

    public BucketMetaDataMapper(String bucketTableName, AmazonDynamoDB dynamoDB, AmazonS3 amazonS3) {
        this.bucketTableName = bucketTableName;
        this.dynamoDB = dynamoDB;
        if (amazonS3 == null) {
            this.amazonS3Optional = Optional.empty();
        } else {
            this.amazonS3Optional = Optional.of(amazonS3);
        }
    }

    public void createBucketTable(List<AttributeDefinition> attributeDefinitionList) {
        boolean tableExist = false;
        try {
            ListTablesResult listTablesResult = dynamoDB.listTables();
            tableExist = CollectionUtils.isNotEmpty(listTablesResult.getTableNames()) && listTablesResult.getTableNames().contains(bucketTableName);
        } catch (Exception e) {
        }

        if (tableExist) {
            return;
        }

        try {
            CreateTableRequest createTableRequest = new CreateTableRequest();
            createTableRequest.setTableName(bucketTableName);

            List<KeySchemaElement> keySchemaElementList = new LinkedList<>();
            keySchemaElementList.add(new KeySchemaElement(KEY_BUCKET_ID, KeyType.HASH));
            keySchemaElementList.add(new KeySchemaElement(KEY_START_BUCKET_WINDOW, KeyType.RANGE));
            createTableRequest.setKeySchema(keySchemaElementList);

            createTableRequest.setAttributeDefinitions(attributeDefinitionList);

            createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(100L, 100L));

            dynamoDB.createTable(createTableRequest);
        } catch (Exception e) {
            log.error("failed create bucket table", e);
        }

        try {
            amazonS3Optional.ifPresent(amazonS3 -> amazonS3.createBucket("addx-test"));
        } catch (Exception e) {
            log.error("failed create s3 bucket", e);
        }

    }

    public <W> boolean isBucketExist(String bucketId, W startBucketWindow) {
        GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.setTableName(bucketTableName);
        getItemRequest.setConsistentRead(false);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
        getItemRequest.setKey(bucketKeyAttributeValueMap);

        getItemRequest.setAttributesToGet(Collections.singletonList(KEY_ITEM_COUNT));

        boolean bucketExist = false;
        try {
            GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
            bucketExist = MapUtils.isNotEmpty(getItemResult.getItem());
        } catch (ResourceNotFoundException e) {
        }
        return bucketExist;
    }

    public <W> boolean isBucketFull(String bucketId, W startBucketWindow) {
        GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.setTableName(bucketTableName);
        getItemRequest.setConsistentRead(false);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
        getItemRequest.setKey(bucketKeyAttributeValueMap);

        getItemRequest.setAttributesToGet(Collections.singletonList(KEY_ITEM_COUNT));

        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
        if (getItemResult == null) {
            return false;
        }

        return (getItemResult == null || getItemResult.getItem() == null) ? false : Integer.valueOf(getItemResult.getItem().get(KEY_ITEM_COUNT).getN()) >= MAX_BUCKET_ITEM_COUNT;
    }

    public <W> void createBucket(String bucketId, W startBucketWindow) {
        PutItemRequest putItemRequest = new PutItemRequest();
        putItemRequest.setTableName(bucketTableName);

        Map<String, AttributeValue> bucketAttributeValueMap = new HashMap<>();
        bucketAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
        bucketAttributeValueMap.put(KEY_ITEM_COUNT, new AttributeValue().withN("0"));
        IntStream.range(0, MAX_BUCKET_ITEM_COUNT).forEach(i -> bucketAttributeValueMap.put(String.join("", KEY_ITEM_MAP, String.valueOf(i)), new AttributeValue().withM(Collections.emptyMap())));
        putItemRequest.setItem(bucketAttributeValueMap);

        currentBucketWindowMap.put(bucketId, startBucketWindow);

        amazonS3Optional.ifPresent(amazonS3 -> {
            String bucketS3StorageKey = String.join("/", bucketTableName, String.valueOf(bucketId), startBucketWindow.toString(), String.join("", String.valueOf(System.currentTimeMillis()), ".json"));
            amazonS3.putObject("addx-test", bucketS3StorageKey, EmptyInputStream.INSTANCE, new ObjectMetadata());
            bucketAttributeValueMap.put(KEY_BUCKET_S3_STORAGE_URL, new AttributeValue().withS(bucketS3StorageKey));
        });

        dynamoDB.putItem(putItemRequest);
        return;
    }

    public <W> W getCurrentBucketWindow(String bucketId) {
        return (W) currentBucketWindowMap.get(bucketId);
    }

    public <W> void initIndex(String bucketId, W startBucketWindow, IndexCollection indexCollection) {
        if (indexCollection == null) {
            return;
        }

        // 先过滤已存在的
        Iterator<Map.Entry<String, IndexCollection.InvertedIndex>> indexEntryIterator = indexCollection.getIndexMap().entrySet().iterator();
        while (indexEntryIterator.hasNext()) {
            Map.Entry<String, IndexCollection.InvertedIndex> indexEntry = indexEntryIterator.next();
            Iterator<Map.Entry<String, IndexCollection.IndexItemId>> indexValueEntryIterator = indexEntry.getValue().getInvertedIndexValueMap().entrySet().iterator();
            while (indexValueEntryIterator.hasNext()) {
                Map.Entry<String, IndexCollection.IndexItemId> indexValueEntry = indexValueEntryIterator.next();
                String indexUniqKey = String.join("_", bucketId, String.valueOf(startBucketWindow), indexEntry.getKey(), indexValueEntry.getKey());
                if (bucketIndexSet.contains(indexUniqKey)) {
                    indexValueEntryIterator.remove();
                } else {
                    bucketIndexSet.add(indexUniqKey);
                }
            }

            if (MapUtils.isEmpty(indexCollection.getIndexMap().get(indexEntry.getKey()).getInvertedIndexValueMap())) {
                indexEntryIterator.remove();
            }
        }

        if (MapUtils.isEmpty(indexCollection.getIndexMap())) {
            return;
        }

        // todo update interval设置, 避免频繁调用更新api

        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        updateItemRequest.setTableName(bucketTableName);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
        updateItemRequest.setKey(bucketKeyAttributeValueMap);

        Map<String, String> expressionNameMap = new HashMap<>();
        String updateExpression = indexCollection.getIndexMap().keySet().stream().map(key -> {
            String keyName = String.join("", "#", key);
            expressionNameMap.put(keyName, key);
            return keyName;
        }).map(key -> String.join("", key, " = ", "if_not_exists(", key, ", :emptyMap)")).collect(Collectors.joining(","));
        updateItemRequest.setUpdateExpression(String.join(" ", "SET", updateExpression));
        updateItemRequest.setExpressionAttributeNames(expressionNameMap);
        updateItemRequest.withExpressionAttributeValues(Collections.singletonMap(":emptyMap", new AttributeValue().withM(Collections.emptyMap())));
        dynamoDB.updateItem(updateItemRequest);

        Map<String, String> subAttributeNameMap = new HashMap<>();
        StringBuilder subUpdateExpressionBuilder = new StringBuilder("SET ");
        indexCollection.getIndexMap().entrySet().forEach(indexEntry -> {
            if (MapUtils.isEmpty(indexEntry.getValue().getInvertedIndexValueMap())) {
                return;
            }

            String indexKey = String.join("", "#", indexEntry.getKey());

            AtomicInteger i = new AtomicInteger();
            indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                if (isEmptyIndexValue(invertedIndexValueEntry.getKey())) {
                    return;
                }

                String indexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                String invertedIndexValueKeyPath = String.join(".", indexKey, indexSubKey);
                subUpdateExpressionBuilder.append(String.join("", invertedIndexValueKeyPath, " = ", "if_not_exists(", invertedIndexValueKeyPath, ", :emptyMap)"));
                subUpdateExpressionBuilder.append(",");
                subAttributeNameMap.put(indexSubKey, invertedIndexValueEntry.getKey());
                subAttributeNameMap.put(indexKey, indexEntry.getKey());
            });
        });

        subUpdateExpressionBuilder.deleteCharAt(subUpdateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(subUpdateExpressionBuilder.toString());
        updateItemRequest.withExpressionAttributeNames(subAttributeNameMap);
        updateItemRequest.withExpressionAttributeValues(Collections.singletonMap(":emptyMap", new AttributeValue().withM(Collections.emptyMap())));
        dynamoDB.updateItem(updateItemRequest);
    }

    boolean isEmptyIndexValue(String str) {
        return SS_EMPTY_STR.equals(str) || NS_EMPTY_VALUE.equals(str);
    }
}
