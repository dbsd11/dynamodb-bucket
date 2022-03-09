package group.bison.dynamodb.bucket.metadata;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.Data;

import java.util.Map;

@Data
public abstract class BucketItem {

    private Map<String, AttributeValue> itemAttributeValueMap;

    private String itemId;

    private String bizId;

    private IndexCollection indexCollection;

    public abstract String getBucketId();

    public abstract <W> W getBucketWindow();
}
