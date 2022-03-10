package group.bison.dynamodb.bucket.simple;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import group.bison.dynamodb.bucket.common.domain.DataQueryParam;
import group.bison.dynamodb.bucket.metadata.BucketItem;
import group.bison.dynamodb.bucket.metadata.IndexCollection;
import group.bison.dynamodb.bucket.parse.ItemParser;
import group.bison.dynamodb.bucket.simple.annotation.BucketIdField;
import group.bison.dynamodb.bucket.simple.annotation.BucketIndexField;
import group.bison.dynamodb.bucket.simple.annotation.ItemTimestampField;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
@AllArgsConstructor
public class SimpleItemParser<T> implements ItemParser<T> {

    private DynamoDBMapperTableModel<T> tableModel;

    @Override
    public Object hashKey(T item) {
        return tableModel.hashKey().get(item);
    }

    @Override
    public Object rangeKey(T item) {
        return tableModel.rangeKey().get(item);
    }

    @Override
    public BucketItem parseItem(T item) {
        BucketItem bucketItem = new SimpleBucketItem();
        bucketItem.setItemAttributeValueMap(tableModel.convert(item));

        Object hashKey = hashKey(item);
        Object rangeKey = rangeKey(item);
        String itemId = getItemId(hashKey, rangeKey);
        bucketItem.setItemId(itemId);

        IndexCollection indexCollection = new IndexCollection();
        bucketItem.setIndexCollection(indexCollection);

        Map<String, AttributeValue> attributeValueMap = this.tableModel.convert(item);
        attributeValueMap.remove(tableModel.hashKey().name());
        attributeValueMap.remove(tableModel.rangeKey().name());

        IndexCollection.IndexItemId indexItemId = new IndexCollection.IndexItemId();
        indexItemId.setItemId(itemId);

        attributeValueMap.entrySet().forEach(stringAttributeValueEntry -> {
            Field bucketIndexField = Arrays.asList(tableModel.targetType().getDeclaredFields()).stream().filter(field -> field.getAnnotation(BucketIndexField.class) != null).filter(field -> stringAttributeValueEntry.getKey().replaceAll("_", "").equalsIgnoreCase(field.getName())).findAny().orElse(null);
            if (bucketIndexField == null) {
                return;
            }

            if (StringUtils.isNotEmpty(stringAttributeValueEntry.getValue().getS())) {
                IndexCollection.InvertedIndex index = new IndexCollection.InvertedIndex();
                index.getInvertedIndexValueMap().put(stringAttributeValueEntry.getValue().getS(), indexItemId);
                indexCollection.getIndexMap().put(stringAttributeValueEntry.getKey(), index);
            }

            if (StringUtils.isNotEmpty(stringAttributeValueEntry.getValue().getN())) {
                IndexCollection.InvertedIndex index = new IndexCollection.InvertedIndex();
                index.getInvertedIndexValueMap().put(stringAttributeValueEntry.getValue().getN(), indexItemId);
                indexCollection.getIndexMap().put(stringAttributeValueEntry.getKey(), index);
            }

            if (CollectionUtils.isNotEmpty(stringAttributeValueEntry.getValue().getNS())) {
                stringAttributeValueEntry.getValue().getNS().forEach(value -> {
                    IndexCollection.InvertedIndex index = new IndexCollection.InvertedIndex();
                    index.getInvertedIndexValueMap().put(value, indexItemId);
                    indexCollection.getIndexMap().put(stringAttributeValueEntry.getKey(), index);
                });
            }

            if (CollectionUtils.isNotEmpty(stringAttributeValueEntry.getValue().getSS())) {
                stringAttributeValueEntry.getValue().getSS().forEach(value -> {
                    IndexCollection.InvertedIndex index = new IndexCollection.InvertedIndex();
                    index.getInvertedIndexValueMap().put(value, indexItemId);
                    indexCollection.getIndexMap().put(stringAttributeValueEntry.getKey(), index);
                });
            }
        });

        return bucketItem;
    }

    @Override
    public T convert2Item(BucketItem bucketItem) {
        if (bucketItem == null) {
            return null;
        }

        T item = tableModel.unconvert(bucketItem.getItemAttributeValueMap());
        try {
            Field bizIdField = item.getClass().getField("bizId");
            if (bizIdField != null) {
                bizIdField.setAccessible(true);
                bizIdField.set(item, bucketItem.getItemAttributeValueMap().get("bizId").getS());
            }
        } catch (Exception e) {
        }

        return item;
    }

    public String getBucketId(T item) {
        Field bucketIdField = Arrays.asList(item.getClass().getDeclaredFields()).stream().filter(field -> field.getAnnotation(BucketIdField.class) != null).findAny().orElse(null);

        String bucketId = null;
        if (bucketIdField != null) {
            try {
                bucketIdField.setAccessible(true);
                bucketId = String.valueOf(bucketIdField.get(item));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return bucketId;
    }

    public Long getTimestamp(T item) {
        Field itemTimestampField = Arrays.asList(item.getClass().getDeclaredFields()).stream().filter(field -> field.getAnnotation(ItemTimestampField.class) != null).findAny().orElse(null);

        Long timestamp = null;
        if (itemTimestampField != null) {
            try {
                itemTimestampField.setAccessible(true);
                timestamp = Long.valueOf(String.valueOf(itemTimestampField.get(item)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return timestamp != null ? (timestamp > Math.pow(10, 11) ? timestamp / 1000 : timestamp) : null;
    }

    public String getItemId(Object hashKey, Object rangeKey) {
        return String.join("_", String.valueOf(hashKey), String.valueOf(rangeKey));
    }

    public List<String> getQueryBucketIdList(DataQueryParam dataQueryParam) {
        Field bucketIdField = Arrays.asList(tableModel.targetType().getDeclaredFields()).stream().filter(field -> field.getAnnotation(BucketIdField.class) != null).findAny().orElse(null);
        if (bucketIdField == null) {
            return Collections.emptyList();
        }

        String bucketIdExpression = dataQueryParam.getExpressionMap().entrySet().stream().filter(expressionEntry -> expressionEntry.getKey().replaceAll("_", "").equalsIgnoreCase(bucketIdField.getName())).map(expressionEntry -> expressionEntry.getValue()).findAny().orElse(null);
        if (StringUtils.isEmpty(bucketIdExpression)) {
            return Collections.emptyList();
        }

        List<String> bucketIdList = dataQueryParam.getExpressionValueMap().entrySet().stream().map(expressionValueEntry -> {
            if (bucketIdExpression.contains(expressionValueEntry.getKey())) {
                return StringUtils.defaultString(expressionValueEntry.getValue().getS(), expressionValueEntry.getValue().getN());
            }
            return null;
        }).filter(obj -> obj != null).collect(Collectors.toList());
        return bucketIdList;
    }

    public Pair<Long, Long> getQueryTimestampRange(DataQueryParam dataQueryParam) {
        Field itemTimestampField = Arrays.asList(tableModel.targetType().getDeclaredFields()).stream().filter(field -> field.getAnnotation(ItemTimestampField.class) != null).findAny().orElse(null);
        if (itemTimestampField == null) {
            return Pair.of(0L, Long.MAX_VALUE);
        }

        String itemTimestampExpression = dataQueryParam.getExpressionMap().entrySet().stream().filter(expressionEntry -> expressionEntry.getKey().replaceAll("_", "").equalsIgnoreCase(itemTimestampField.getName())).map(expressionEntry -> expressionEntry.getValue()).findAny().orElse(null);
        if (StringUtils.isEmpty(itemTimestampExpression)) {
            return Pair.of(0L, Long.MAX_VALUE);
        }

        List<Long> bucketTimestampList = dataQueryParam.getExpressionValueMap().entrySet().stream().map(expressionValueEntry -> {
            if (itemTimestampExpression.contains(expressionValueEntry.getKey())) {
                return Long.valueOf(expressionValueEntry.getValue().getN());
            }
            return null;
        }).filter(obj -> obj != null).collect(Collectors.toList());
        Long minItemTimestamp = bucketTimestampList.stream().mapToLong(Long::longValue).min().orElse(0);
        Long maxItemTimestamp = bucketTimestampList.stream().mapToLong(Long::longValue).max().orElse(Long.MAX_VALUE);
        return Pair.of(minItemTimestamp, maxItemTimestamp);
    }

    public IndexCollection getQueryIndexCollection(DataQueryParam dataQueryParam) {
        IndexCollection queryIndexCollection = new IndexCollection();

        dataQueryParam.getExpressionMap().entrySet().forEach(expressionEntry -> {
            Field bucketIndexField = Arrays.asList(tableModel.targetType().getDeclaredFields()).stream().filter(field -> field.getAnnotation(BucketIndexField.class) != null).filter(field -> expressionEntry.getKey().replaceAll("_", "").equalsIgnoreCase(field.getName())).findAny().orElse(null);
            if (bucketIndexField == null) {
                return;
            }

            queryIndexCollection.getIndexMap().put(expressionEntry.getKey(), new IndexCollection.InvertedIndex());

            String expression = expressionEntry.getValue();
            dataQueryParam.getExpressionValueMap().entrySet().forEach(expressionValueEntry -> {
                if (!expression.contains(expressionValueEntry.getKey())) {
                    return;
                }

                String indexValueKey = StringUtils.defaultString(expressionValueEntry.getValue().getS(), expressionValueEntry.getValue().getN());
                queryIndexCollection.getIndexMap().get(expressionEntry.getKey()).getInvertedIndexValueMap().put(indexValueKey, null);
            });
        });

        return queryIndexCollection;
    }
}
