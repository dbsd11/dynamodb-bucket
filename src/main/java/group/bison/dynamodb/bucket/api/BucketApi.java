package group.bison.dynamodb.bucket.api;

import group.bison.dynamodb.bucket.common.domain.DataQueryParam;

import java.util.List;

public interface BucketApi<T> {

    public String add(T item);

    public void update(String bizId, T item);

    public void delete(String bizId, Object hashKey, Object rangeKey);

    public T queryOne(String bizId, Object hashKey, Object rangeKey);

    public List<T> query(DataQueryParam dataQueryParam, T latestItem);
}
