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
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_COUNT;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_MAP;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class BucketMetaDataMapper {

    private String bucketTableName;

    private AmazonDynamoDB dynamoDB;

    private static Set<String> bucketIndexSet = new HashSet<>();

    public void createBucketTable(List<AttributeDefinition> attributeDefinitionList) {
        boolean tableExist = false;
        try {
            ListTablesResult listTablesResult = dynamoDB.listTables(bucketTableName);
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

            createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(10L, 10L));

            dynamoDB.createTable(createTableRequest);
        } catch (Exception e) {
            log.error("failed create bucket table", e);
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

        return (getItemResult == null || getItemResult.getItem() == null) ? false : Integer.valueOf(getItemResult.getItem().get(KEY_ITEM_COUNT).getN()) > 100;
    }

    public <W> void createBucket(String bucketId, W startBucketWindow) {
        PutItemRequest putItemRequest = new PutItemRequest();
        putItemRequest.setTableName(bucketTableName);

        Map<String, AttributeValue> bucketAttributeValueMap = new HashMap<>();
        bucketAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
        bucketAttributeValueMap.put(KEY_ITEM_COUNT, new AttributeValue().withN("0"));
        bucketAttributeValueMap.put(KEY_ITEM_MAP, new AttributeValue().withM(Collections.emptyMap()));
        putItemRequest.setItem(bucketAttributeValueMap);

        dynamoDB.putItem(putItemRequest);
        return;
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

        String updateExpression = indexCollection.getIndexMap().keySet().stream().map(key -> String.join("", key, " = ", "if_not_exists(", key, ", :emptyMap)")).collect(Collectors.joining(","));
        updateItemRequest.setUpdateExpression(String.join(" ", "SET", updateExpression));
        updateItemRequest.withExpressionAttributeValues(Collections.singletonMap(":emptyMap", new AttributeValue().withM(Collections.emptyMap())));
        dynamoDB.updateItem(updateItemRequest);

        Map<String, String> subAttributeNameMap = new HashMap<>();
        StringBuilder subUpdateExpressionBuilder = new StringBuilder("SET ");
        indexCollection.getIndexMap().entrySet().forEach(indexEntry -> {
            if (MapUtils.isEmpty(indexEntry.getValue().getInvertedIndexValueMap())) {
                return;
            }

            String indexKey = indexEntry.getKey();
            AtomicInteger i = new AtomicInteger();
            indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                String indexSubKey = String.join("", "#", indexKey, String.valueOf(i.incrementAndGet()));
                String invertedIndexValueKeyPath = String.join(".", indexKey, indexSubKey);
                subUpdateExpressionBuilder.append(String.join("", invertedIndexValueKeyPath, " = ", "if_not_exists(", invertedIndexValueKeyPath, ", :emptyMap)"));
                subUpdateExpressionBuilder.append(",");
                subAttributeNameMap.put(indexSubKey, invertedIndexValueEntry.getKey());
            });
        });

        subUpdateExpressionBuilder.deleteCharAt(subUpdateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(subUpdateExpressionBuilder.toString());
        updateItemRequest.withExpressionAttributeNames(subAttributeNameMap);
        updateItemRequest.withExpressionAttributeValues(Collections.singletonMap(":emptyMap", new AttributeValue().withM(Collections.emptyMap())));
        dynamoDB.updateItem(updateItemRequest);
    }
}
