package group.bison.dynamodb.bucket.simple;

import group.bison.dynamodb.bucket.metadata.BucketItem;
import org.apache.commons.lang3.StringUtils;

/**
 * bizId： ${prefix}-${bucketId}-${startBucketWindow}-${suffix}
 *
 * @param <T>
 */
public class SimpleBucketItem extends BucketItem {

    @Override
    public String getBucketId() {
        String bizId = getBizId();
        if (StringUtils.isEmpty(bizId)) {
            return null;
        }
        String[] bizIdArray = bizId.split("-");
        return bizIdArray.length >= 2 ? bizId.split("-")[1] : null;
    }

    @Override
    public Long getBucketWindow() {
        String bizId = getBizId();
        if (StringUtils.isEmpty(bizId)) {
            return null;
        }
        String[] bizIdArray = bizId.split("-");
        return bizIdArray.length >= 3 ? Long.valueOf(bizId.split("-")[2]) : null;
    }
}
