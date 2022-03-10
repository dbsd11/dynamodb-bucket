package group.bison.dynamodb.bucket.test.util;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DynamodbJsonConverter implements DynamoDBTypeConverter<AttributeValue, Object> {

    @Override
    public AttributeValue convert(Object value) {
        if (value == null) return new AttributeValue().withNULL(true);
        if (value instanceof String) return new AttributeValue().withS((String) value);
        if (value instanceof Boolean) return new AttributeValue().withBOOL((Boolean) value);
        if (value instanceof Number) return new AttributeValue().withN(value + "");
        if (value instanceof byte[]) return new AttributeValue().withB(ByteBuffer.wrap((byte[]) value));
        if (value instanceof Date) return new AttributeValue().withS(convertDate((Date) value));
        if (value instanceof Collection) {
            AttributeValue list = new AttributeValue().withL(new LinkedList<>());
            ((Collection) value).forEach(v -> list.getL().add(convert(v)));
            return list;
        }
        if (value instanceof Map) {
            AttributeValue map = new AttributeValue();
            ((Map<String, Object>) value).forEach((k, v) -> map.addMEntry(k, convert(v)));
            return map;
        }
        log.error("DynamoJsonConverter convert 不可转换的值:{}", value);
        return null;
    }

    public static String convertDate(Date date) {
        return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(date);
    }

    @Override
    public Object unconvert(AttributeValue value) {
        if(value == null) return null;
        if (value.getS() != null) return value.getS();
        if (value.getM() != null) return value.getM().entrySet().stream()
                .collect(Collectors.toMap(it -> it.getKey(), it -> unconvert(it.getValue())));
        if (value.getSS() != null) return value.getSS();
        if (value.getL() != null) return value.getL().stream().map(this::unconvert).collect(Collectors.toList());
        if (value.getBOOL() != null) return value.getBOOL();
        if (value.getNS() != null) return value.getNS().stream().map(BigDecimal::new).collect(Collectors.toList());
        if (value.getB() != null) return value.getB().array();
        if (value.getBS() != null) return value.getBS().stream().map(it -> it.array()).collect(Collectors.toList());
        if (value.getN() != null) return new BigDecimal(value.getN());
        if (value.getNULL() != null && value.getNULL()) return null;
        return null;
    }
}
