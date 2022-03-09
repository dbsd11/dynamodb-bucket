package group.bison.dynamodb.bucket.metadata;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class IndexCollection {

    private Map<String, InvertedIndex> indexMap = new HashMap<>();

    @Data
    public static class InvertedIndex {
        private Map<String, IndexItemId> invertedIndexValueMap = new HashMap<>();
    }

    @Data
    public static class IndexItemId {
        private String itemId;
    }
}
