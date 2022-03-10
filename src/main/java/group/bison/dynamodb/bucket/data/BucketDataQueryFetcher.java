package group.bison.dynamodb.bucket.data;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import group.bison.dynamodb.bucket.common.domain.DataQueryParam;
import group.bison.dynamodb.bucket.metadata.BucketItem;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.MapUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BIZ_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_ITEM_MAP;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;

@NoArgsConstructor
@AllArgsConstructor
public class BucketDataQueryFetcher {

    private String bucketTableName;

    private AmazonDynamoDB dynamoDB;

    private ExpressionFilter expressionFilter;

    public List<BucketItem> fetch(String bucketId, AttributeValue startBucketWindow, DataQueryParam dataQueryParam) {
        GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.setTableName(bucketTableName);
        getItemRequest.setConsistentRead(false);

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow);
        getItemRequest.setKey(bucketKeyAttributeValueMap);

        getItemRequest.setProjectionExpression(KEY_ITEM_MAP);

        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
        Map<String, BucketItem> bucketItemMap = parseGetItemResult(getItemResult);

        List<BucketItem> matchBucketItemList = filterDataQueryExpression(bucketItemMap.values(), dataQueryParam);
        return matchBucketItemList;
    }

    public List<BucketItem> fetch(String bucketId, AttributeValue startBucketWindow, Collection<String> queryItemIds, DataQueryParam dataQueryParam) {
        GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.setTableName(bucketTableName);
        getItemRequest.setConsistentRead(false);

        Map<String, String> itemQueryAttributeNameMap = new HashMap<>();

        Map<String, AttributeValue> bucketKeyAttributeValueMap = new HashMap<>();
        bucketKeyAttributeValueMap.put(KEY_BUCKET_ID, new AttributeValue().withS(bucketId));
        bucketKeyAttributeValueMap.put(KEY_START_BUCKET_WINDOW, startBucketWindow);
        getItemRequest.setKey(bucketKeyAttributeValueMap);

        StringBuilder projectExpressionBuilder = new StringBuilder();
        AtomicInteger i = new AtomicInteger();

        queryItemIds.forEach(bucketItemId -> {
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
        Map<String, BucketItem> bucketItemMap = parseGetItemResult(getItemResult);

        List<BucketItem> matchBucketItemList = filterDataQueryExpression(bucketItemMap.values(), dataQueryParam);
        return matchBucketItemList;
    }

    Map<String, BucketItem> parseGetItemResult(GetItemResult getItemResult) {
        if (getItemResult == null || MapUtils.isEmpty(getItemResult.getItem()) || !getItemResult.getItem().containsKey(KEY_ITEM_MAP)) {
            return Collections.emptyMap();
        }

        Map<String, BucketItem> bucketItemMap = getItemResult.getItem().get(KEY_ITEM_MAP).getM().entrySet().stream().map(itemAttributeValueEntry -> {
            String itemId = itemAttributeValueEntry.getKey();
            Map<String, AttributeValue> attributeValueMap = itemAttributeValueEntry.getValue().getM();
            if (MapUtils.isEmpty(attributeValueMap)) {
                return null;
            }


            if (MapUtils.isEmpty(itemAttributeValueEntry.getValue().getM())) {
                return null;
            }

            BucketItem bucketItem = new BucketItem() {
                @Override
                public String getBucketId() {
                    return null;
                }

                @Override
                public <W> W getBucketWindow() {
                    return null;
                }
            };
            bucketItem.setItemId(itemId);
            bucketItem.setBizId(attributeValueMap.get(KEY_BIZ_ID).getS());
            bucketItem.setItemAttributeValueMap(attributeValueMap);
            return bucketItem;
        }).filter(obj -> obj != null).collect(Collectors.toMap(BucketItem::getItemId, Function.identity()));
        return bucketItemMap;
    }

    List<BucketItem> filterDataQueryExpression(Collection<BucketItem> queryBucketItemList, DataQueryParam dataQueryParam) {
        // apply expression filter
        List<BucketItem> matchBucketItemList = queryBucketItemList.stream().filter(bucketItem -> {
            if (dataQueryParam == null || MapUtils.isEmpty(dataQueryParam.getExpressionMap()) || expressionFilter == null) {
                return true;
            }

            return bucketItem.getItemAttributeValueMap().entrySet().stream().filter(attributeValueEntry -> dataQueryParam.getExpressionMap().containsKey(attributeValueEntry.getKey()))
                    .allMatch(attributeValueEntry -> expressionFilter.isMatch(dataQueryParam.getExpressionMap().get(attributeValueEntry.getKey()), dataQueryParam.getExpressionNameMap(), dataQueryParam.getExpressionValueMap(), attributeValueEntry.getValue()));
        }).collect(Collectors.toList());
        return matchBucketItemList;
    }
}
