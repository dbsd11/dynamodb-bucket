package group.bison.dynamodb.bucket.data;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnItemCollectionMetrics;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import group.bison.dynamodb.bucket.common.Constants;
import group.bison.dynamodb.bucket.common.domain.DataQueryParam;
import group.bison.dynamodb.bucket.metadata.BucketItem;
import group.bison.dynamodb.bucket.metadata.IndexCollection;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
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

import static group.bison.dynamodb.bucket.common.Constants.KEY_BIZ_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_MAP;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;
import static group.bison.dynamodb.bucket.common.Constants.KEY_TTL_TIMESTAMP;
import static group.bison.dynamodb.bucket.common.Constants.MAX_BUCKET_ITEM_COUNT;
import static group.bison.dynamodb.bucket.common.Constants.NS_EMPTY_VALUE;
import static group.bison.dynamodb.bucket.common.Constants.SCAN_MAX_COUNT;
import static group.bison.dynamodb.bucket.common.Constants.SS_EMPTY_STR;

@NoArgsConstructor
public class BucketDataMapper {

    private String bucketTableName;

    private AmazonDynamoDB dynamoDB;

    private ExpressionFilter expressionFilter;

    private BucketDataQueryFetcher bucketDataQueryFetcher;

    public BucketDataMapper(String bucketTableName, AmazonDynamoDB dynamoDB, ExpressionFilter expressionFilter) {
        this.bucketTableName = bucketTableName;
        this.dynamoDB = dynamoDB;
        this.expressionFilter = expressionFilter;

        init();
    }

    void init() {
        this.bucketDataQueryFetcher = new BucketDataQueryFetcher(bucketTableName, dynamoDB, expressionFilter);
    }

    public void insert(BucketItem bucketItem) {
        // 需保存bizId
        bucketItem.getItemAttributeValueMap().put(KEY_BIZ_ID, new AttributeValue().withS(bucketItem.getBizId()));

        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        updateItemRequest.setTableName(bucketTableName);
        updateItemRequest.setReturnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketItem.getBucketId()));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, bucketItem.getBucketWindow() instanceof String ? (new AttributeValue().withS(bucketItem.getBucketWindow())) : new AttributeValue().withN(String.valueOf(bucketItem.<Object>getBucketWindow())));
        updateItemRequest.setKey(bucketKeyAttributeValueMap);

        Map<String, AttributeValueUpdate> updateAttributeValueMap = new HashMap<>();
        updateAttributeValueMap.put("ttl_timestamp", new AttributeValueUpdate().withValue(new AttributeValue().withN("1")));
        updateItemRequest.setAttributeUpdates(updateAttributeValueMap);

        updateItemRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        UpdateItemResult updateItemResult2 = dynamoDB.updateItem(updateItemRequest);
        System.out.println("updateItemResult2:" + updateItemResult2);

        Map<String, String> attributeNameMap = new HashMap<>();
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        StringBuilder updateExpressionBuilder = new StringBuilder("SET ");

        String itemMapColumn = getItemMapColumn(bucketItem.getItemId());
        updateExpressionBuilder.append(String.join("", itemMapColumn, ".", "#itemId", " = ", ":item"));
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

                String indexKey = String.join("", "#", indexEntry.getKey());

                AtomicInteger i = new AtomicInteger();
                indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                    if (isEmptyIndexValue(invertedIndexValueEntry.getKey())) {
                        return;
                    }

                    String indexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                    String invertedIndexValueKeyPath = String.join(".", indexKey, indexSubKey, "#itemId");
                    updateExpressionBuilder.append(String.join("", invertedIndexValueKeyPath, " = ", ":one"));
                    updateExpressionBuilder.append(",");
                    attributeNameMap.put(indexSubKey, invertedIndexValueEntry.getKey());
                    attributeNameMap.put(indexKey, indexEntry.getKey());
                });
            });
        }

        if (bucketItem.getItemAttributeValueMap().containsKey(KEY_TTL_TIMESTAMP)) {
            updateExpressionBuilder.append("ttl_timestamp").append("=").append(":ttl_timestamp");
            updateExpressionBuilder.append(",");
            attributeValueMap.put(":ttl_timestamp", bucketItem.getItemAttributeValueMap().get(KEY_TTL_TIMESTAMP));
        }

        updateExpressionBuilder.deleteCharAt(updateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(updateExpressionBuilder.toString());
        updateItemRequest.setExpressionAttributeNames(attributeNameMap);
        updateItemRequest.setExpressionAttributeValues(attributeValueMap);

        updateItemRequest.setAttributeUpdates(null);
        updateItemRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        UpdateItemResult updateItemResult = dynamoDB.updateItem(updateItemRequest);
        System.out.println("updateItemResult:" + updateItemResult);
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

        String itemMapColumn = getItemMapColumn(bucketItem.getItemId());

        bucketItem.getItemAttributeValueMap().entrySet().forEach(updateItemAttributeEntry -> {
            String updateItemAttributeKey = String.join("", "#", updateItemAttributeEntry.getKey());
            String updateItemAttributeValueKey = String.join("", ":", updateItemAttributeEntry.getKey());
            updateExpressionBuilder.append(String.join("", itemMapColumn, ".", "#itemId", ".", updateItemAttributeKey, " = ", updateItemAttributeValueKey));
            updateExpressionBuilder.append(",");
            attributeNameMap.put(updateItemAttributeKey, updateItemAttributeEntry.getKey());
            attributeValueMap.put(updateItemAttributeValueKey, updateItemAttributeEntry.getValue());
        });

        attributeNameMap.put("#itemId", bucketItem.getItemId());

        // add value index
        if (bucketItem.getIndexCollection() != null) {
            // query current value
            BucketItem currentBucketItem = queryOne(bucketItem.getBucketId(), bucketItem.getBucketWindow(), bucketItem.getItemId());

            bucketItem.getIndexCollection().getIndexMap().entrySet().forEach(indexEntry -> {
                if (MapUtils.isEmpty(indexEntry.getValue().getInvertedIndexValueMap())) {
                    return;
                }

                String indexKey = String.join("", "#", indexEntry.getKey());

                AtomicInteger i = new AtomicInteger();

                AttributeValue currentAttributeValue = currentBucketItem.getItemAttributeValueMap().get(indexEntry.getKey());
                if (CollectionUtils.isNotEmpty(currentAttributeValue.getSS())) {
                    currentAttributeValue.getSS().forEach(currentValue -> {
                        if (isEmptyIndexValue(currentValue)) {
                            return;
                        }

                        if (indexEntry.getValue().getInvertedIndexValueMap().containsKey(currentValue)) {
                            return;
                        }

                        String currentIndexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                        updateExpressionBuilder.append(String.join("", indexKey, ".", currentIndexSubKey, ".", "#itemId", " = ", ":zero"));
                        updateExpressionBuilder.append(",");
                        attributeNameMap.put(currentIndexSubKey, currentValue);
                        attributeNameMap.put(indexKey, indexEntry.getKey());
                        attributeValueMap.put(":zero", new AttributeValue().withN("0"));
                    });
                    indexEntry.getValue().getInvertedIndexValueMap().keySet().forEach(value -> {
                        if (!currentAttributeValue.getSS().contains(value)) {
                            // set new index value 1
                            String newIndexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                            updateExpressionBuilder.append(String.join("", indexKey, ".", newIndexSubKey, ".", "#itemId", " = ", ":one"));
                            updateExpressionBuilder.append(",");
                            attributeNameMap.put(newIndexSubKey, value);
                            attributeNameMap.put(indexKey, indexEntry.getKey());
                            attributeValueMap.put(":one", new AttributeValue().withN("1"));
                        }
                    });
                } else if (CollectionUtils.isNotEmpty(currentAttributeValue.getNS())) {
                    currentAttributeValue.getNS().forEach(currentValue -> {
                        if (isEmptyIndexValue(currentValue)) {
                            return;
                        }

                        if (indexEntry.getValue().getInvertedIndexValueMap().containsKey(currentValue)) {
                            return;
                        }

                        String currentIndexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                        updateExpressionBuilder.append(String.join("", indexKey, ".", currentIndexSubKey, ".", "#itemId", " = ", ":zero"));
                        updateExpressionBuilder.append(",");
                        attributeNameMap.put(currentIndexSubKey, currentValue);
                        attributeNameMap.put(indexKey, indexEntry.getKey());
                        attributeValueMap.put(":zero", new AttributeValue().withN("0"));
                    });

                    indexEntry.getValue().getInvertedIndexValueMap().keySet().forEach(value -> {
                        if (!currentAttributeValue.getNS().contains(value)) {
                            // set new index value 1
                            String newIndexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                            updateExpressionBuilder.append(String.join("", indexKey, ".", newIndexSubKey, ".", "#itemId", " = ", ":one"));
                            updateExpressionBuilder.append(",");
                            attributeNameMap.put(newIndexSubKey, value);
                            attributeNameMap.put(indexKey, indexEntry.getKey());
                            attributeValueMap.put(":one", new AttributeValue().withN("1"));
                        }
                    });
                } else {
                    indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                        // set old index value 0
                        String currentValue = StringUtils.defaultString(currentAttributeValue.getS(), currentAttributeValue.getN());
                        if (isEmptyIndexValue(currentValue)) {
                            return;
                        }
                        if (StringUtils.equals(invertedIndexValueEntry.getKey(), currentValue)) {
                            return;
                        }

                        String currentIndexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                        updateExpressionBuilder.append(String.join("", indexKey, ".", currentIndexSubKey, ".", "#itemId", " = ", ":zero"));
                        updateExpressionBuilder.append(",");
                        attributeNameMap.put(currentIndexSubKey, currentValue);
                        attributeValueMap.put(":zero", new AttributeValue().withN("0"));

                        // set new index value 1
                        String newIndexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                        updateExpressionBuilder.append(String.join("", indexKey, ".", newIndexSubKey, ".", "#itemId", " = ", ":one"));
                        updateExpressionBuilder.append(",");
                        attributeNameMap.put(newIndexSubKey, invertedIndexValueEntry.getKey());
                        attributeValueMap.put(":one", new AttributeValue().withN("1"));

                        attributeNameMap.put(indexKey, indexEntry.getKey());
                    });
                }
            });
        }

        updateExpressionBuilder.deleteCharAt(updateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(updateExpressionBuilder.toString());
        updateItemRequest.setExpressionAttributeNames(attributeNameMap);
        updateItemRequest.setExpressionAttributeValues(attributeValueMap);

        updateItemRequest.setConditionExpression(String.join("", "attribute_exists(", itemMapColumn, ".", "#itemId", ".", KEY_BIZ_ID, ")"));

        updateItemRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        UpdateItemResult updateItemResult = dynamoDB.updateItem(updateItemRequest);
        System.out.println("updateItem " + updateItemResult.getConsumedCapacity());
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

        String itemMapColumn = getItemMapColumn(bucketItem.getItemId());

        updateExpressionBuilder.append(String.join("", itemMapColumn, ".", "#itemId", " = ", ":emptyMap"));
        updateExpressionBuilder.append(",");
        attributeNameMap.put("#itemId", bucketItem.getItemId());
        attributeValueMap.put(":emptyMap", new AttributeValue().withM(Collections.emptyMap()));

        updateExpressionBuilder.append(String.join("", Constants.KEY_ITEM_COUNT, " = ", Constants.KEY_ITEM_COUNT, " - ", ":one"));
        updateExpressionBuilder.append(",");
        attributeValueMap.put(":one", new AttributeValue().withN("1"));

        updateExpressionBuilder.deleteCharAt(updateExpressionBuilder.length() - 1);

        updateItemRequest.setUpdateExpression(updateExpressionBuilder.toString());
        updateItemRequest.setExpressionAttributeNames(attributeNameMap);
        updateItemRequest.setExpressionAttributeValues(attributeValueMap);

        dynamoDB.updateItem(updateItemRequest);
    }

    public <W> BucketItem queryOne(String bucketId, W startBucketWindow, String itemId) {
        List<BucketItem> bucketItemList = bucketDataQueryFetcher.fetch(bucketId, startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)), Collections.singleton(itemId), null);
        return CollectionUtils.isNotEmpty(bucketItemList) ? bucketItemList.get(0) : null;
    }

    public <W> List<BucketItem> query(String bucketId, W startBucketWindow, W endBucketWindow, IndexCollection indexCollection, DataQueryParam dataQueryParam) {
        if (dataQueryParam == null) {
            return Collections.emptyList();
        }

        if (dataQueryParam.getTo() <= dataQueryParam.getFrom()) {
            return Collections.emptyList();
        }

        // 返回的bucketItemList结果
        List<BucketItem> bucketItemList = new LinkedList<>();

        // 尝试直接在最新的bucket查询
        boolean eagerFetched = false;
        if (dataQueryParam.getTo() < MAX_BUCKET_ITEM_COUNT) {
            eagerFetched = true;
            List<BucketItem> matchBucketItemList = bucketDataQueryFetcher.fetch(bucketId, endBucketWindow instanceof String ? new AttributeValue().withS((String) endBucketWindow) : new AttributeValue().withN(String.valueOf(endBucketWindow)), dataQueryParam);
            if (CollectionUtils.isNotEmpty(matchBucketItemList)) {
                bucketItemList.addAll(matchBucketItemList);
            }
        }
        if (bucketItemList.size() >= dataQueryParam.getTo()) {
            bucketItemList = new ArrayList<>(bucketItemList);
            return bucketItemList.subList(Math.min(bucketItemList.size(), dataQueryParam.getFrom()), Math.min(bucketItemList.size(), dataQueryParam.getTo()));
        }

        // 根据索引行为不同走不同的逻辑
        if (indexCollection == null || MapUtils.isEmpty(indexCollection.getIndexMap())) {
            // 直接遍历bucket
            QueryRequest bucketQueryRequest = new QueryRequest();
            bucketQueryRequest.setTableName(bucketTableName);
            bucketQueryRequest.setConsistentRead(false);
            bucketQueryRequest.setScanIndexForward(false);

            Map<String, AttributeValue> bucketQueryAttributeValueMap = new HashMap<>();

            bucketQueryRequest.setKeyConditionExpression(String.join("", KEY_BUCKET_ID, "=", ":bucketId", " AND ", KEY_START_BUCKET_WINDOW, " BETWEEN ", ":startBucketWindow", " AND ", ":endBucketWindow"));
            bucketQueryAttributeValueMap.put(":bucketId", new AttributeValue().withS(bucketId));
            bucketQueryAttributeValueMap.put(":startBucketWindow", startBucketWindow instanceof String ? new AttributeValue().withS((String) startBucketWindow) : new AttributeValue().withN(String.valueOf(startBucketWindow)));
            bucketQueryAttributeValueMap.put(":endBucketWindow", endBucketWindow instanceof String ? new AttributeValue().withS((String) endBucketWindow) : new AttributeValue().withN(String.valueOf(endBucketWindow)));

            StringBuilder invertIndexProjectExpression = new StringBuilder();
            invertIndexProjectExpression.append(KEY_BUCKET_ID).append(",").append(KEY_START_BUCKET_WINDOW);

            bucketQueryRequest.setProjectionExpression(invertIndexProjectExpression.toString());
            bucketQueryRequest.setExpressionAttributeValues(bucketQueryAttributeValueMap);

            QueryResult queryResult = dynamoDB.query(bucketQueryRequest);
            List<Map<String, AttributeValue>> bucketMapList = queryResult.getItems();

            Iterator<Map<String, AttributeValue>> bucketMapListIterator = bucketMapList.iterator();
            if (eagerFetched && bucketItemList.size() != 0 && bucketMapListIterator.hasNext()) {
                bucketMapListIterator.next();
            }

            while (bucketMapListIterator.hasNext()) {
                Map<String, AttributeValue> bucketMap = bucketMapListIterator.next();

                AttributeValue bucketWindowAttributeValue = bucketMap.get(KEY_START_BUCKET_WINDOW);

                List<BucketItem> matchBucketItemList = bucketDataQueryFetcher.fetch(bucketId, bucketWindowAttributeValue, dataQueryParam);

                if (CollectionUtils.isNotEmpty(matchBucketItemList)) {
                    bucketItemList.addAll(matchBucketItemList);
                }

                if (bucketItemList.size() >= dataQueryParam.getTo()) {
                    break;
                }
            }
        } else {
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

                String indexKey = String.join("", "#", indexEntry.getKey());

                AtomicInteger i = new AtomicInteger();
                indexEntry.getValue().getInvertedIndexValueMap().entrySet().forEach(invertedIndexValueEntry -> {
                    if (isEmptyIndexValue(invertedIndexValueEntry.getKey())) {
                        return;
                    }

                    String indexSubKey = String.join("", indexKey, String.valueOf(i.incrementAndGet()));
                    invertIndexProjectExpression.append(String.join("", indexKey, ".", indexSubKey));
                    invertIndexProjectExpression.append(",");
                    invertIndexAttributeNameMap.put(indexSubKey, invertedIndexValueEntry.getKey());
                    invertIndexAttributeNameMap.put(indexKey, indexEntry.getKey());
                });
            });

            invertIndexProjectExpression.deleteCharAt(invertIndexProjectExpression.length() - 1);

            invertIndexQueryRequest.setProjectionExpression(invertIndexProjectExpression.toString());
            invertIndexQueryRequest.setExpressionAttributeNames(invertIndexAttributeNameMap);
            invertIndexQueryRequest.setExpressionAttributeValues(invertIndexAttributeValueMap);

            QueryResult queryResult = dynamoDB.query(invertIndexQueryRequest);
            List<Map<String, AttributeValue>> invertIndexMapList = queryResult.getItems();

            Iterator<Map<String, AttributeValue>> invertIndexMapListIterator = invertIndexMapList.iterator();
            if (eagerFetched && bucketItemList.size() != 0 && invertIndexMapListIterator.hasNext()) {
                invertIndexMapListIterator.next();
            }

            // 倒排索引命中的itemId统计
            AtomicInteger scannedCount = new AtomicInteger();
            while (invertIndexMapListIterator.hasNext()) {
                if (scannedCount.get() >= SCAN_MAX_COUNT) {
                    break;
                }

                Map<String, AttributeValue> invertIndexMap = invertIndexMapListIterator.next();

                AttributeValue bucketWindowAttributeValue = invertIndexMap.get(KEY_START_BUCKET_WINDOW);

                Set<String> bucketItemIdSet = new HashSet<>();

                invertIndexMap.entrySet().forEach(invertIndexEntry -> {
                    if (!indexCollection.getIndexMap().containsKey(invertIndexEntry.getKey())) {
                        return;
                    }
                    if (MapUtils.isEmpty(invertIndexEntry.getValue().getM()) || !invertIndexEntry.getValue().getM().keySet().containsAll(indexCollection.getIndexMap().get(invertIndexEntry.getKey()).getInvertedIndexValueMap().keySet())) {
                        bucketItemIdSet.clear();
                        return;
                    }

                    invertIndexEntry.getValue().getM().values().forEach(invertIndexValue -> {
                        List<String> validItemIdList = invertIndexValue.getM().entrySet().stream().filter(entry -> "1".equals(entry.getValue().getN())).map(entry -> entry.getKey()).collect(Collectors.toList());
                        if (CollectionUtils.isEmpty(bucketItemIdSet)) {
                            bucketItemIdSet.addAll(validItemIdList);
                        } else {
                            bucketItemIdSet.retainAll(validItemIdList);
                        }
                    });
                });

                if (bucketItemIdSet.size() == 0) {
                    continue;
                }

                scannedCount.getAndAdd(bucketItemIdSet.size());

                // begin query item
                if (MapUtils.isNotEmpty(dataQueryParam.getExpressionMap())) {
                    Map<String, String> expressionMap = new HashMap<>(dataQueryParam.getExpressionMap());
                    indexCollection.getIndexMap().keySet().forEach(index -> {
                        expressionMap.remove(index);
                    });
                    dataQueryParam.setExpressionMap(expressionMap);
                }

                List<BucketItem> matchBucketItemList = bucketDataQueryFetcher.fetch(bucketId, bucketWindowAttributeValue, bucketItemIdSet, dataQueryParam);
                if (CollectionUtils.isNotEmpty(matchBucketItemList)) {
                    bucketItemList.addAll(matchBucketItemList);
                }

                if (bucketItemList.size() >= dataQueryParam.getTo()) {
                    break;
                }
            }
        }

        bucketItemList = new ArrayList<>(bucketItemList);
        return bucketItemList.subList(Math.min(bucketItemList.size(), dataQueryParam.getFrom()), Math.min(bucketItemList.size(), dataQueryParam.getTo()));
    }

    String getItemMapColumn(String itemId) {
        int h = 0;
        int hash = (itemId == null) ? 0 : (h = itemId.hashCode()) ^ (h >>> 16);
        return String.join("", KEY_ITEM_MAP, String.valueOf((MAX_BUCKET_ITEM_COUNT - 1) & hash));
    }

    boolean isEmptyIndexValue(String str) {
        return SS_EMPTY_STR.equals(str) || NS_EMPTY_VALUE.equals(str);
    }
}
