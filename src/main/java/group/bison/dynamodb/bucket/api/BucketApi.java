package group.bison.dynamodb.bucket.api;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.List;
import java.util.Map;

public interface BucketApi<T> {

    public String add(T item);

    public void update(String bizId, T item);

    public void delete(String bizId, Object hashKey, Object rangeKey);

    public T queryOne(String bizId, Object hashKey, Object rangeKey);

    public List<T> query(Map<String, String> expressionMap, Map<String, String> expressionNameMap, Map<String, AttributeValue> expressionValueMap, int from, int to, T latestItem);
}