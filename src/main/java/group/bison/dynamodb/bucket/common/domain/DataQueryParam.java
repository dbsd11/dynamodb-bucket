package group.bison.dynamodb.bucket.common.domain;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class DataQueryParam {
    private Map<String, String> expressionMap;

    private Map<String, String> expressionNameMap;

    private Map<String, AttributeValue> expressionValueMap;

    private int from;

    private int to;
}
