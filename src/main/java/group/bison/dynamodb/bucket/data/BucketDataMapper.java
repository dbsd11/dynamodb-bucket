package group.bison.dynamodb.bucket.data;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import group.bison.dynamodb.bucket.common.Constants;
import group.bison.dynamodb.bucket.metadata.BucketItem;
import group.bison.dynamodb.bucket.metadata.IndexCollection;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
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
import java.util.stream.Stream;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_MAP;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;

@NoArgsConstructor
@AllArgsConstructor
public class BucketDataMapper {

    private String bucketTableName;

    private DynamoDBMapper dynamoDBMapper;

    private AmazonDynamoDB dynamoDB;

    private static final String KEY_BIZ_ID = "bizId";

    public void insert(BucketItem bucketItem) {
        // 需保存bizId
        bucketItem.getItemAttributeValueMap().put(KEY_BIZ_ID, new AttributeValue().withS(bucketItem.getBizId()));

        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        updateItemRequest.setTableName(bucketTableName);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketItem.getBucketId()));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, bucketItem.getBucketWindow() instanceof String ? (new AttributeValue().withS(bucketItem.getBucketWindow())) : new AttributeValue().withN(String.valueOf(bucketItem.<Object>getBucketWindow())));
        updateItemRequest.setKey(bucketKeyAttributeValueMap);

        Map<String, String> attributeNameMap = new HashMap<>();
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        StringBuilder updateExpressionBuilder = new StringBuilder("SET ");

        updateExpressionBuilder.append(String.join("", Constants.KEY_ITEM_MAP, ".", "#itemId", " = ", ":item"));
        updateExpressionBuilder.append(",");
        attributeNameMap.put("#itemId", bucketItem.getItemId());
        attributeValueMap.put(":item", new AttributeValue().withM(bucketItem.getItemAttributeValueMap()));

        updateExpressionBuilder.append(String.join("", Constants.KEY_ITEM_COUNT, " = ", Constants.KEY_ITEM_COUNT, " + ", ":one"));
        updateExpressionBuilder.append(",");
        attributeValueMap.put(":one", new AttributeValue().withN("1"));

        // add value index
        if (bucketItem.getIndexCollection() != null) {
            bucketItem.getIndexCollection().getIndexMap().entrySet().forEach(indexEntry -> {
                if (MapUtils.isEmpty(indexEntry.getValue().getInvertedIndexValueMap())) {
                    return;
                }

                String indexKey = indexEntry.getKey();
                AtomicInteger i = new AtomicInteger();
                indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                    String indexSubKey = String.join("", "#", indexKey, String.valueOf(i.incrementAndGet()));
                    String invertedIndexValueKeyPath = String.join(".", indexKey, indexSubKey, "#itemId");
                    updateExpressionBuilder.append(String.join("", invertedIndexValueKeyPath, " = ", ":one"));
                    updateExpressionBuilder.append(",");
                    attributeNameMap.put(indexSubKey, invertedIndexValueEntry.getKey());
                });
            });
        }

        updateExpressionBuilder.deleteCharAt(updateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(updateExpressionBuilder.toString());
        updateItemRequest.setExpressionAttributeNames(attributeNameMap);
        updateItemRequest.setExpressionAttributeValues(attributeValueMap);

        dynamoDB.updateItem(updateItemRequest);
    }

    public void update(BucketItem bucketItem) {
        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        updateItemRequest.setTableName(bucketTableName);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketItem.getBucketId()));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, bucketItem.getBucketWindow() instanceof String ? new AttributeValue().withS((String) bucketItem.getBucketWindow()) : new AttributeValue().withN(String.valueOf(bucketItem.<Object>getBucketWindow())));
        updateItemRequest.setKey(bucketKeyAttributeValueMap);

        Map<String, String> attributeNameMap = new HashMap<>();
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        StringBuilder updateExpressionBuilder = new StringBuilder("SET ");

        updateExpressionBuilder.append(String.join("", Constants.KEY_ITEM_MAP, ".", "#itemId", " = ", ":item"));
        updateExpressionBuilder.append(",");
        attributeNameMap.put("#itemId", bucketItem.getItemId());
        attributeValueMap.put(":item", new AttributeValue().withM(bucketItem.getItemAttributeValueMap()));

        // add value index
        if (bucketItem.getIndexCollection() != null) {
            bucketItem.getIndexCollection().getIndexMap().entrySet().forEach(indexEntry -> {
                if (MapUtils.isEmpty(indexEntry.getValue().getInvertedIndexValueMap())) {
                    return;
                }

                String indexKey = indexEntry.getKey();
                AtomicInteger i = new AtomicInteger();
                indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                    String indexSubKey = String.join("", "#", indexKey, String.valueOf(i.incrementAndGet()));
                    String invertedIndexValueKeyPath = String.join(".", indexKey, indexSubKey, "#itemId");
                    updateExpressionBuilder.append(String.join("", invertedIndexValueKeyPath, " = ", ":one"));
                    updateExpressionBuilder.append(",");
                    attributeNameMap.put(indexSubKey, invertedIndexValueEntry.getKey());
                    attributeValueMap.put(":one", new AttributeValue().withN("1"));
                });
            });
        }

        updateExpressionBuilder.deleteCharAt(updateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(updateExpressionBuilder.toString());
        updateItemRequest.setExpressionAttributeNames(attributeNameMap);
        updateItemRequest.setExpressionAttributeValues(attributeValueMap);

        updateItemRequest.setConditionExpression(String.join("", "attribute_exists(", KEY_ITEM_MAP, ".", "#itemId", ".", KEY_BIZ_ID, ")"));

        dynamoDB.updateItem(updateItemRequest);
    }

    public void delete(BucketItem bucketItem) {
        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        updateItemRequest.setTableName(bucketTableName);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketItem.getBucketId()));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, bucketItem.getBucketWindow() instanceof String ? new AttributeValue().withS((String) bucketItem.getBucketWindow()) : new AttributeValue().withN(String.valueOf(bucketItem.<Object>getBucketWindow())));
        updateItemRequest.setKey(bucketKeyAttributeValueMap);

        Map<String, String> attributeNameMap = new HashMap<>();
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        StringBuilder updateExpressionBuilder = new StringBuilder("SET ");

        updateExpressionBuilder.append(String.join("", Constants.KEY_ITEM_MAP, ".", "#itemId", " = ", ":emptyMap"));
        updateExpressionBuilder.append(",");
        attributeNameMap.put("#itemId", bucketItem.getItemId());
        attributeValueMap.put(":emptyMap", new AttributeValue().withM(Collections.emptyMap()));

        updateExpressionBuilder.append(String.join("", Constants.KEY_ITEM_COUNT, " = ", Constants.KEY_ITEM_COUNT, " - ", ":one"));
        updateExpressionBuilder.append(",");
        attributeValueMap.put(":one", new AttributeValue().withN("1"));


//        BucketItem existBucketItem = queryOne(bucketItem.getBucketId(), bucketItem.getBucketWindow(), bucketItem.getItemId());
//        if (existBucketItem == null) {
//            return;
//        }
//        AtomicInteger i = new AtomicInteger();
//        existBucketItem.getItemAttributeValueMap().entrySet().forEach(itemAttributeValueEntry -> {
//            if (StringUtils.isNotEmpty(itemAttributeValueEntry.getValue().getS())) {
//                String itemValueKey = String.join("", "#", String.valueOf(i.incrementAndGet()));
//                updateExpressionBuilder.append(String.join("", itemAttributeValueEntry.getKey(), ".", itemValueKey, ".", "#itemId", " = ", ":emptyMap"));
//                updateExpressionBuilder.append(",");
//                attributeNameMap.put(itemValueKey, itemAttributeValueEntry.getValue().getS());
//            }
//
//            if (StringUtils.isNotEmpty(itemAttributeValueEntry.getValue().getN())) {
//                String itemValueKey = String.join("", "#", String.valueOf(i.incrementAndGet()));
//                updateExpressionBuilder.append(String.join("", itemAttributeValueEntry.getKey(), ".", itemValueKey, ".", "#itemId", " = ", ":emptyMap"));
//                updateExpressionBuilder.append(",");
//                attributeNameMap.put(itemValueKey, itemAttributeValueEntry.getValue().getN());
//            }
//
//            if (CollectionUtils.isNotEmpty(itemAttributeValueEntry.getValue().getSS())) {
//                itemAttributeValueEntry.getValue().getSS().forEach(value -> {
//                    String itemValueKey = String.join("", "#", String.valueOf(i.incrementAndGet()));
//                    updateExpressionBuilder.append(String.join("", itemAttributeValueEntry.getKey(), ".", itemValueKey, ".", "#itemId", " = ", ":emptyMap"));
//                    updateExpressionBuilder.append(",");
//                    attributeNameMap.put(itemValueKey, value);
//                });
//            }
//
//            if (CollectionUtils.isNotEmpty(itemAttributeValueEntry.getValue().getNS())) {
//                itemAttributeValueEntry.getValue().getNS().forEach(value -> {
//                    String itemValueKey = String.join("", "#", String.valueOf(i.incrementAndGet()));
//                    updateExpressionBuilder.append(String.join("", itemAttributeValueEntry.getKey(), ".", itemValueKey, ".", "#itemId", " = ", ":emptyMap"));
//                    updateExpressionBuilder.append(",");
//                    attributeNameMap.put(itemValueKey, value);
//                });
//            }
//        });

        updateExpressionBuilder.deleteCharAt(updateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(updateExpressionBuilder.toString());
        updateItemRequest.setExpressionAttributeNames(attributeNameMap);
        updateItemRequest.setExpressionAttributeValues(attributeValueMap);

        dynamoDB.updateItem(updateItemRequest);
    }

    public <W> BucketItem queryOne(String bucketId, W startBucketWindow, String itemId) {
        GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.setTableName(bucketTableName);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
        getItemRequest.setKey(bucketKeyAttributeValueMap);

        getItemRequest.setProjectionExpression(String.join("", KEY_ITEM_MAP, ".", "#itemId"));

        getItemRequest.setExpressionAttributeNames(Collections.singletonMap("#itemId", itemId));

        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
        if (getItemResult == null
                || MapUtils.isEmpty(getItemResult.getItem()) || !getItemResult.getItem().containsKey(KEY_ITEM_MAP)
                || MapUtils.isEmpty(getItemResult.getItem().get(KEY_ITEM_MAP).getM()) || !getItemResult.getItem().get(KEY_ITEM_MAP).getM().containsKey(itemId)
                || MapUtils.isEmpty(getItemResult.getItem().get(KEY_ITEM_MAP).getM().get(itemId).getM())) {
            return null;
        }

        Map<String, AttributeValue> attributeValueMap = getItemResult.getItem().get(KEY_ITEM_MAP).getM().get(itemId).getM();

        BucketItem bucketItem = new BucketItem() {
            @Override
            public String getBucketId() {
                return bucketId;
            }

            @Override
            public <W> W getBucketWindow() {
                return (W) startBucketWindow;
            }
        };
        bucketItem.setBizId(attributeValueMap.get(KEY_BIZ_ID).getS());
        bucketItem.setItemAttributeValueMap(attributeValueMap);
        return bucketItem;
    }

    public <W> List<BucketItem> query(String bucketId, W startBucketWindow, W endBucketWindow, IndexCollection indexCollection, int from, int to) {
        if (to <= from) {
            return Collections.emptyList();
        }

        if (indexCollection == null) {
            // todo get all item
            return Collections.emptyList();
        }

        // 先通过倒排索引筛选查询itemId集合
        QueryRequest invertIndexQueryRequest = new QueryRequest();
        invertIndexQueryRequest.setTableName(bucketTableName);
        invertIndexQueryRequest.setConsistentRead(false);
        invertIndexQueryRequest.setScanIndexForward(false);

        Map<String, String> invertIndexAttributeNameMap = new HashMap<>();
        Map<String, AttributeValue> invertIndexAttributeValueMap = new HashMap<>();

        invertIndexQueryRequest.setKeyConditionExpression(String.join("", KEY_BUCKET_ID, "=", ":bucketId", " AND ", KEY_START_BUCKET_WINDOW, " BETWEEN ", ":startBucketWindow", " AND ", ":endBucketWindow"));
        invertIndexAttributeValueMap.put(":bucketId", new AttributeValue().withS(bucketId));
        invertIndexAttributeValueMap.put(":startBucketWindow", startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
        invertIndexAttributeValueMap.put(":endBucketWindow", endBucketWindow instanceof String ? new AttributeValue().withS((String) endBucketWindow) : new AttributeValue().withN(String.valueOf(endBucketWindow)));

        StringBuilder invertIndexProjectExpression = new StringBuilder();
        invertIndexProjectExpression.append(KEY_BUCKET_ID).append(",").append(KEY_START_BUCKET_WINDOW).append(",");
        indexCollection.getIndexMap().entrySet().forEach(indexEntry -> {
            if (MapUtils.isEmpty(indexEntry.getValue().getInvertedIndexValueMap())) {
                return;
            }

            String indexKey = indexEntry.getKey();
            AtomicInteger i = new AtomicInteger();
            indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                String indexSubKey = String.join("", "#", indexKey, String.valueOf(i.incrementAndGet()));
                String invertedIndexValueKeyPath = String.join(".", indexKey, indexSubKey);
                invertIndexProjectExpression.append(String.join("", invertedIndexValueKeyPath));
                invertIndexProjectExpression.append(",");
                invertIndexAttributeNameMap.put(indexSubKey, invertedIndexValueEntry.getKey());
            });
        });

        invertIndexProjectExpression.deleteCharAt(invertIndexProjectExpression.length() - 1);

        invertIndexQueryRequest.setProjectionExpression(invertIndexProjectExpression.toString());
        invertIndexQueryRequest.setExpressionAttributeNames(invertIndexAttributeNameMap);
        invertIndexQueryRequest.setExpressionAttributeValues(invertIndexAttributeValueMap);

        QueryResult queryResult = dynamoDB.query(invertIndexQueryRequest);

        List<Map<String, AttributeValue>> invertIndexMapList = queryResult.getItems();

        // 倒排索引命中的itemId集合
        List<String> itemIdList = new LinkedList<>();
        Map<String, AttributeValue> itemId2BucketWindowMap = new HashMap<>();

        Iterator<Map<String, AttributeValue>> invertIndexMapListIterator = invertIndexMapList.iterator();
        while (invertIndexMapListIterator.hasNext()) {
            if (itemIdList.size() >= (to - from)) {
                break;
            }

            Set<String> bucketItemIdSet = new HashSet<>();

            Map<String, AttributeValue> invertIndexMap = invertIndexMapListIterator.next();

            invertIndexMap.remove(KEY_BUCKET_ID).getS();
            AttributeValue bucketWindowAttributeValue = invertIndexMap.remove(KEY_START_BUCKET_WINDOW);

            invertIndexMap.values().forEach(invertIndex -> {
                if (MapUtils.isEmpty(invertIndex.getM())) {
                    return;
                }

                invertIndex.getM().values().forEach(invertIndexValue -> {
                    if (CollectionUtils.isEmpty(bucketItemIdSet)) {
                        bucketItemIdSet.addAll(invertIndexValue.getM().keySet());
                    } else {
                        bucketItemIdSet.retainAll(invertIndexValue.getM().keySet());
                    }
                });
            });

            itemIdList.addAll(bucketItemIdSet);

            bucketItemIdSet.forEach(bucketItemId -> itemId2BucketWindowMap.put(bucketItemId, bucketWindowAttributeValue));
        }

        // 没有匹配的itemId或者超过匹配集合大小
        if (CollectionUtils.isEmpty(itemIdList) || (from >= itemIdList.size())) {
            return Collections.emptyList();
        }

        List<String> queryItemIdList = itemIdList.subList(from, Math.min(itemIdList.size(), to));

        // 按照bucketWindow分组
        Map<String, AttributeValue> queryBucketWindowMap = new HashMap<>();
        Map<String, List<String>> queryBucketWindowItemIdMap = new HashMap<>();
        queryItemIdList.stream().forEach(queryItemId -> {
            AttributeValue queryBucketWindow = itemId2BucketWindowMap.get(queryItemId);
            String queryBucketWindowKey = startBucketWindow instanceof String ? queryBucketWindow.getS() : queryBucketWindow.getN();
            queryBucketWindowMap.put(queryBucketWindowKey, queryBucketWindow);

            if (!queryBucketWindowItemIdMap.containsKey(queryBucketWindowKey)) {
                queryBucketWindowItemIdMap.put(queryBucketWindowKey, new LinkedList<>());
            }
            queryBucketWindowItemIdMap.get(queryBucketWindowKey).add(queryItemId);
        });

        List<BucketItem> bucketItemList = queryBucketWindowMap.entrySet().stream().flatMap(queryBucketWindowEntry -> {
            List<String> bucketItemIdList = queryBucketWindowItemIdMap.get(queryBucketWindowEntry.getKey());

            // begin query item
            GetItemRequest getItemRequest = new GetItemRequest();
            getItemRequest.setTableName(bucketTableName);
            getItemRequest.setConsistentRead(false);

            Map<String, String> itemQueryAttributeNameMap = new HashMap<>();

            Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
            bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
            bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, queryBucketWindowEntry.getValue());
            getItemRequest.setKey(bucketKeyAttributeValueMap);

            StringBuilder projectExpressionBuilder = new StringBuilder();
            AtomicInteger i = new AtomicInteger();

            bucketItemIdList.forEach(bucketItemId -> {
                String itemIdKey = String.join("", "#itemId", String.valueOf(i.incrementAndGet()));
                projectExpressionBuilder.append(String.join("", KEY_ITEM_MAP, ".", itemIdKey));
                projectExpressionBuilder.append(",");
                itemQueryAttributeNameMap.put(itemIdKey, bucketItemId);
                return;
            });

            projectExpressionBuilder.deleteCharAt(projectExpressionBuilder.length() - 1);

            getItemRequest.setProjectionExpression(projectExpressionBuilder.toString());
            getItemRequest.setExpressionAttributeNames(itemQueryAttributeNameMap);

            GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
            if (getItemResult == null || MapUtils.isEmpty(getItemResult.getItem()) || !getItemResult.getItem().containsKey(KEY_ITEM_MAP)) {
                return Stream.empty();
            }

            return getItemResult.getItem().get(KEY_ITEM_MAP).getM().values().stream().map(itemAttributeValueMap -> {
                if (MapUtils.isEmpty(itemAttributeValueMap.getM())) {
                    return null;
                }
                BucketItem bucketItem = new BucketItem() {
                    @Override
                    public String getBucketId() {
                        return bucketId;
                    }

                    @Override
                    public <W> W getBucketWindow() {
                        return null;
                    }
                };

                bucketItem.setBizId(itemAttributeValueMap.getM().get(KEY_BIZ_ID).getS());
                bucketItem.setItemAttributeValueMap(itemAttributeValueMap.getM());
                return bucketItem;
            });
        }).filter(obj -> obj != null).collect(Collectors.toList());

        return bucketItemList;
    }
}
