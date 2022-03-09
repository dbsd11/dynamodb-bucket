package group.bison.dynamodb.bucket.parse;

import group.bison.dynamodb.bucket.metadata.BucketItem;

public interface ItemParser<T> {

    public Object hashKey(T item);

    public Object rangeKey(T item);

    public BucketItem parseItem(T item);

    public T convert2Item(BucketItem bucketItem);

}
