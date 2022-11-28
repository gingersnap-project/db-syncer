package io.gingersnapproject.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec;
import io.gingersnapproject.proto.api.config.v1alpha1.Key;
import io.gingersnapproject.proto.api.config.v1alpha1.KeyFormat;
import io.gingersnapproject.proto.api.config.v1alpha1.NamespacedRef;
import io.gingersnapproject.proto.api.config.v1alpha1.Value;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class JSONMappingTest {
    private static String eRuleTestCaseJSON;
    private static EagerCacheRuleSpec eRule;

    @BeforeAll
    public static void init() {
        eRuleTestCaseJSON = "{\n" +
                "  \"cacheRef\": {\n" +
                "    \"name\": \"myCache\",\n" +
                "    \"namespace\": \"myNamespace\"\n" +
                "  },\n" +
                "  \"tableName\": \"TABLE_EAGER_RULE_1\",\n" +
                "  \"key\": {\n" +
                "    \"format\": \"JSON\",\n" +
                "    \"keySeparator\": \",\",\n" +
                "    \"keyColumns\": [\"col1\", \"col3\", \"col4\"]\n" +
                "  },\n" +
                "  \"value\": {\n" +
                "    \"valueColumns\": [\"col6\", \"col7\", \"col8\"]\n" +
                "  }\n" +
                "}";

        EagerCacheRuleSpec.Builder eRuleBuilder = EagerCacheRuleSpec.newBuilder();
        NamespacedRef.Builder ns = NamespacedRef.newBuilder().setName("myCache").setNamespace("myNamespace");
        // Populating resources
        // Populating key
        List<String> keyColumns = Arrays.asList("col1", "col3", "col4");
        Key.Builder keyBuilder = Key.newBuilder()
                .setFormat(KeyFormat.JSON)
                .setKeySeparator(",")
                .addAllKeyColumns(keyColumns);
        // Populating value
        List<String> valueColumns = Arrays.asList("col6", "col7", "col8");
        Value.Builder valueBuilder = Value.newBuilder()
                .addAllValueColumns(valueColumns);
        // Assembling Eager Rule
        eRuleBuilder.setTableName("TABLE_EAGER_RULE_1");
        // Adding key
        eRuleBuilder.setKey(keyBuilder);
        // Adding value
        eRuleBuilder.setValue(valueBuilder);
        // Adding ref to the cache
        eRuleBuilder.setCacheRef(ns);
        eRule = eRuleBuilder.build();
    }

    @Test
    public void testProtobufToJSON() {
        try {
            String eRuleJSON = JsonFormat.printer().print(eRule);
            assertEquals(eRuleTestCaseJSON, eRuleJSON);
        } catch (InvalidProtocolBufferException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testJSONToProtobuf() {
        EagerCacheRuleSpec.Builder eRuleBuilder = EagerCacheRuleSpec.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(eRuleTestCaseJSON, eRuleBuilder);
            EagerCacheRuleSpec eRuleFromJson = eRuleBuilder.build();
            assertEquals(eRule, eRuleFromJson);
        } catch (InvalidProtocolBufferException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
