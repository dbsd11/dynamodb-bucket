package group.bison.dynamodb.bucket.common;

public interface Constants {

    String KEY_BUCKET_ID = "bucket_id";

    String KEY_START_BUCKET_WINDOW = "start_bucket_window";

    String KEY_ITEM_COUNT = "item_count";

    String KEY_ITEM_MAP = "item_map";

    String KEY_BUCKET_S3_STORAGE_URL = "s3_storage_url";

    String KEY_BIZ_ID = "bizId";

    String KEY_TTL_TIMESTAMP = "ttl_timestamp";

    String SS_EMPTY_STR = "";

    String NS_EMPTY_VALUE = "-0";

    Integer MAX_BUCKET_ITEM_COUNT = 64;

    Integer SCAN_MAX_COUNT = 10000;
}
