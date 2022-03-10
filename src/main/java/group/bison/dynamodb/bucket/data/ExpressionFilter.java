package group.bison.dynamodb.bucket.data;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;

public interface ExpressionFilter {

    boolean isMatch(String expression, Map<String, String> expressionNameMap, Map<String, AttributeValue> expressionValueMap, AttributeValue currentAttributeValue);
}
