package group.bison.dynamodb.bucket.simple;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import group.bison.dynamodb.bucket.data.ExpressionFilter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleExpressionFilter implements ExpressionFilter {

    @Override
    public boolean isMatch(String expression, Map<String, String> expressionNameMap, Map<String, AttributeValue> expressionValueMap, AttributeValue currentAttributeValue) {
        boolean isMatch = false;

        if (expression.contains("size")) {
            AttributeValue attributeValue = expressionValueMap.entrySet().stream().filter(expressionValueEntry -> expression.contains(expressionValueEntry.getKey())).map(Map.Entry::getValue).findAny().orElse(null);
            int currentSize = currentAttributeValue == null ? 0
                    : StringUtils.isNotEmpty(currentAttributeValue.getS()) ? currentAttributeValue.getS().length()
                    : CollectionUtils.isNotEmpty(currentAttributeValue.getNS()) ? currentAttributeValue.getNS().size()
                    : CollectionUtils.isNotEmpty(currentAttributeValue.getSS()) ? currentAttributeValue.getSS().size()
                    : CollectionUtils.isNotEmpty(currentAttributeValue.getL()) ? currentAttributeValue.getL().size()
                    : MapUtils.isNotEmpty(currentAttributeValue.getM()) ? currentAttributeValue.getM().size()
                    : 0;

            int expectSize = attributeValue == null ? 0 : Integer.valueOf(attributeValue.getN());

            if (expression.contains(">=") || expression.contains("> =")) {
                isMatch = currentSize >= expectSize;
            } else if (expression.contains(">")) {
                isMatch = currentSize > expectSize;
            } else if (expression.contains("<=") || expression.contains("< =")) {
                isMatch = currentSize <= expectSize;
            } else if (expression.contains("<")) {
                isMatch = currentSize < expectSize;
            } else if (expression.contains("=")) {
                isMatch = currentSize == expectSize;
            }
        } else if (expression.contains(">=") || expression.contains("> =")) {
            AttributeValue attributeValue = expressionValueMap.entrySet().stream().filter(expressionValueEntry -> expression.contains(expressionValueEntry.getKey())).map(Map.Entry::getValue).findAny().orElse(null);
            isMatch = attributeValue != null && currentAttributeValue != null && (StringUtils.defaultString(currentAttributeValue.getN(), currentAttributeValue.getS()).compareTo(StringUtils.defaultString(attributeValue.getN(), attributeValue.getS())) >= 0);
        } else if (expression.contains(">")) {
            AttributeValue attributeValue = expressionValueMap.entrySet().stream().filter(expressionValueEntry -> expression.contains(expressionValueEntry.getKey())).map(Map.Entry::getValue).findAny().orElse(null);
            isMatch = attributeValue != null && currentAttributeValue != null && (StringUtils.defaultString(currentAttributeValue.getN(), currentAttributeValue.getS()).compareTo(StringUtils.defaultString(attributeValue.getN(), attributeValue.getS())) > 0);
        } else if (expression.contains("<=") || expression.contains("< =")) {
            AttributeValue attributeValue = expressionValueMap.entrySet().stream().filter(expressionValueEntry -> expression.contains(expressionValueEntry.getKey())).map(Map.Entry::getValue).findAny().orElse(null);
            isMatch = attributeValue != null && currentAttributeValue != null && (StringUtils.defaultString(currentAttributeValue.getN(), currentAttributeValue.getS()).compareTo(StringUtils.defaultString(attributeValue.getN(), attributeValue.getS())) <= 0);
        } else if (expression.contains("<")) {
            AttributeValue attributeValue = expressionValueMap.entrySet().stream().filter(expressionValueEntry -> expression.contains(expressionValueEntry.getKey())).map(Map.Entry::getValue).findAny().orElse(null);
            isMatch = attributeValue != null && currentAttributeValue != null && (StringUtils.defaultString(currentAttributeValue.getN(), currentAttributeValue.getS()).compareTo(StringUtils.defaultString(attributeValue.getN(), attributeValue.getS())) < 0);
        } else if (expression.contains("=")) {
            AttributeValue attributeValue = expressionValueMap.entrySet().stream().filter(expressionValueEntry -> expression.contains(expressionValueEntry.getKey())).map(Map.Entry::getValue).findAny().orElse(null);
            isMatch = attributeValue != null && currentAttributeValue != null && (StringUtils.defaultString(currentAttributeValue.getN(), currentAttributeValue.getS()).compareTo(StringUtils.defaultString(attributeValue.getN(), attributeValue.getS())) == 0);
        } else if (expression.contains("contains")) {
            List<AttributeValue> attributeValueList = expressionValueMap.entrySet().stream().filter(expressionValueEntry -> expression.contains(expressionValueEntry.getKey())).map(Map.Entry::getValue).collect(Collectors.toList());
            if (currentAttributeValue != null && CollectionUtils.isNotEmpty(currentAttributeValue.getNS())) {
                isMatch = attributeValueList.stream().allMatch(attributeValue -> currentAttributeValue.getNS().contains(attributeValue.getN()));
            } else if (currentAttributeValue != null && CollectionUtils.isNotEmpty(currentAttributeValue.getSS())) {
                isMatch = attributeValueList.stream().allMatch(attributeValue -> currentAttributeValue.getSS().contains(attributeValue.getS()));
            } else if (currentAttributeValue != null && StringUtils.isNotEmpty(currentAttributeValue.getS())) {
                isMatch = attributeValueList.stream().allMatch(attributeValue -> currentAttributeValue.getS().contains(attributeValue.getS()));
            } else if (currentAttributeValue != null && MapUtils.isNotEmpty(currentAttributeValue.getM())) {
                isMatch = attributeValueList.stream().allMatch(attributeValue -> currentAttributeValue.getM().containsKey(attributeValue.getS()));
            }
        }

        return isMatch;
    }
}
