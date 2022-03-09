package group.bison.dynamodb.bucket.simple;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.ConversionSchemas;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import group.bison.dynamodb.bucket.api.BucketApi;
import group.bison.dynamodb.bucket.data.BucketDataMapper;
import group.bison.dynamodb.bucket.metadata.BucketItem;
import group.bison.dynamodb.bucket.metadata.BucketMetaDataMapper;
import group.bison.dynamodb.bucket.metadata.IndexCollection;
import group.bison.dynamodb.bucket.parse.ItemParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static group.bison.dynamodb.bucket.common.Constants.KEY_BUCKET_ID;
import static group.bison.dynamodb.bucket.common.Constants.KEY_START_BUCKET_WINDOW;

public class SimpleBucket<T> implements BucketApi<T> {

    private String tableName;
    private Class<T> itemCls;
    private AmazonDynamoDB dynamoDB;
    private ItemParser<T> itemParser;
    private BucketMetaDataMapper bucketMetaDataMapper;
    private BucketDataMapper bucketDataMapper;

    public SimpleBucket(String tableName, Class<T> itemCls, AmazonDynamoDB dynamoDB) {
        this.tableName = tableName;
        this.itemCls = itemCls;
        this.dynamoDB = dynamoDB;

        init();
    }

    void init() {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB, DynamoDBMapperConfig.builder()
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE_SKIP_NULL_ATTRIBUTES)
                .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(tableName))
                .withConversionSchema(ConversionSchemas.V2)
                .build());
        DynamoDBMapperTableModel<T> tableModel = mapper.getTableModel(itemCls);
        this.itemParser = new SimpleItemParser<>(tableModel);
        this.bucketMetaDataMapper = new BucketMetaDataMapper("bucket-" + tableName, dynamoDB);
        this.bucketDataMapper = new BucketDataMapper("bucket-" + tableName, dynamoDB);

        List<AttributeDefinition> attributeDefinitionList = new LinkedList<>();
        attributeDefinitionList.add(new AttributeDefinition().withAttributeName(KEY_BUCKET_ID).withAttributeType(ScalarAttributeType.S));
        attributeDefinitionList.add(new AttributeDefinition().withAttributeName(KEY_START_BUCKET_WINDOW).withAttributeType(ScalarAttributeType.N));
        bucketMetaDataMapper.createBucketTable(attributeDefinitionList);
    }

    @Override
    public String add(T item) {
        Object hashKey = itemParser.hashKey(item);
        if (hashKey == null) {
            throw new RuntimeException("can not add no hash key item");
        }

        BucketItem bucketItem = itemParser.parseItem(item);

        // 确定bucketId 和 bucketWindow
        String bucketId = itemParser instanceof SimpleItemParser ? ((SimpleItemParser<T>) itemParser).getBucketId(item) : String.valueOf(hashKey);
        Long timestamp = itemParser instanceof SimpleItemParser ? ((SimpleItemParser<T>) itemParser).getTimestamp(item) : null;
        Long bucketWindow = (timestamp != null ? timestamp : System.currentTimeMillis() / 1000) / (60 * 60);

        while (bucketMetaDataMapper.isBucketExist(bucketId, bucketWindow) && bucketMetaDataMapper.isBucketFull(bucketId, bucketWindow)) {
            bucketWindow = bucketWindow + 1;
        }

        if (!bucketMetaDataMapper.isBucketExist(bucketId, bucketWindow)) {
            bucketMetaDataMapper.createBucket(bucketId, bucketWindow);
        }

        // 生成bizId
        String bizId = String.join("-", UUID.randomUUID().toString().substring(0, 5), bucketId, String.valueOf(bucketWindow), String.valueOf(System.currentTimeMillis() / 1000));
        bucketItem.setBizId(bizId);

        IndexCollection indexCollection = bucketItem.getIndexCollection();
        bucketMetaDataMapper.initIndex(bucketId, bucketWindow, indexCollection);

        bucketDataMapper.insert(bucketItem);

        return bizId;
    }

    @Override
    public void update(String bizId, T item) {
        BucketItem bucketItem = itemParser.parseItem(item);
        bucketItem.setBizId(bizId);

        String bucketId = bucketItem.getBucketId();
        Long bucketWindow = bucketItem.getBucketWindow();

        IndexCollection indexCollection = bucketItem.getIndexCollection();
        bucketMetaDataMapper.initIndex(bucketId, bucketWindow, indexCollection);

        bucketDataMapper.update(bucketItem);
    }

    @Override
    public void delete(String bizId, Object hashKey, Object rangeKey) {
        BucketItem bucketItem = new SimpleBucketItem();
        bucketItem.setBizId(bizId);
        if (itemParser instanceof SimpleItemParser) {
            String itemId = ((SimpleItemParser<T>) itemParser).getItemId(hashKey, rangeKey);
            bucketItem.setItemId(itemId);
        }

        bucketDataMapper.delete(bucketItem);
    }

    @Override
    public T queryOne(String bizId, Object hashKey, Object rangeKey) {
        BucketItem bucketItem = new SimpleBucketItem();
        bucketItem.setBizId(bizId);
        if (itemParser instanceof SimpleItemParser) {
            String itemId = ((SimpleItemParser<T>) itemParser).getItemId(hashKey, rangeKey);
            bucketItem.setItemId(itemId);
        }

        String bucketId = bucketItem.getBucketId();
        Long bucketWindow = (Long) bucketItem.getBucketWindow();

        BucketItem queryBucketItem = bucketDataMapper.queryOne(bucketId, bucketWindow, bucketItem.getItemId());
        return itemParser instanceof SimpleItemParser ? itemParser.convert2Item(queryBucketItem) : null;
    }

    @Override
    public List<T> query(Map<String, String> expressionMap, Map<String, String> expressionNameMap, Map<String, AttributeValue> expressionValueMap, int from, int to, T latestItem) {
        if (to <= from) {
            return Collections.emptyList();
        }

        List<String> queryBucketIdList = itemParser instanceof SimpleItemParser ? ((SimpleItemParser<T>) itemParser).getQueryBucketIdList(expressionMap, expressionNameMap, expressionValueMap) : null;
        if (CollectionUtils.isEmpty(queryBucketIdList)) {
            return Collections.emptyList();
        }

        Pair<Long, Long> queryTimestampRange = itemParser instanceof SimpleItemParser ? ((SimpleItemParser<T>) itemParser).getQueryTimestampRange(expressionMap, expressionNameMap, expressionValueMap) : null;
        if (queryTimestampRange == null) {
            return Collections.emptyList();
        }

        AtomicReference<String> lastBucketIdAtom = new AtomicReference<>();
        AtomicReference<Long> lastTimestampAtom = new AtomicReference<>();
        if (latestItem != null) {
            String lastBucketId = ((SimpleItemParser<T>) itemParser).getBucketId(latestItem);
            lastBucketIdAtom.set(lastBucketId);
            Long lastTimestamp = ((SimpleItemParser<T>) itemParser).getTimestamp(latestItem);
            lastTimestampAtom.set(lastTimestamp);
        }

        IndexCollection queryIndexCollection = ((SimpleItemParser<T>) itemParser).getQueryIndexCollection(expressionMap, expressionNameMap, expressionValueMap);

        List<T> itemList = new LinkedList<>();
        queryBucketIdList.stream().sorted(String::compareTo).filter(queryBucketId -> StringUtils.isNotEmpty(lastBucketIdAtom.get()) ? queryBucketId.compareTo(lastBucketIdAtom.get()) >= 0 : true).forEach(queryBucketId -> {
            if (itemList.size() >= (to - from)) {
                return;
            }

            List<BucketItem> queryBucketItemList = bucketDataMapper.query(queryBucketId, queryTimestampRange.getLeft() / (60 * 60), Math.min(queryTimestampRange.getRight(), lastTimestampAtom.get() != null ? lastTimestampAtom.get() : Long.MAX_VALUE) / (60 * 60), queryIndexCollection, Math.max(from - itemList.size(), 0), to - itemList.size());
            List<T> queryItemList = itemParser instanceof SimpleItemParser ? queryBucketItemList.stream().map(itemParser::convert2Item).collect(Collectors.toList()) : null;
            if (CollectionUtils.isNotEmpty(queryItemList)) {
                itemList.addAll(queryItemList);
            }
        });
        return itemList.subList(Math.min(itemList.size(), from), Math.min(itemList.size(), to));
    }

}
