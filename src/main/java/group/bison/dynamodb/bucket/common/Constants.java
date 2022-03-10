package group.bison.dynamodb.bucket.common;

public interface Constants {

    String KEY_BUCKET_ID = "bucket_id";

    String KEY_START_BUCKET_WINDOW = "start_bucket_window";

    String KEY_ITEM_COUNT = "item_count";

    String KEY_ITEM_MAP = "item_map";

    String KEY_BIZ_ID = "bizId";

    String KEY_TTL_TIMESTAMP = "ttl_timestamp";

    Integer MAX_BUCKET_ITEM_COUNT = 100;

    Integer SCAN_MAX_COUNT = 10000;
}
