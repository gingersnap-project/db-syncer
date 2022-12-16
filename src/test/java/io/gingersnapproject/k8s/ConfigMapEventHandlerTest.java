package io.gingersnapproject.k8s;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.protobuf.util.JsonFormat;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.gingersnapproject.cdc.DynamicRuleManagement;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.proto.JSONMappingTest;
import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec;
import io.gingersnapproject.proto.api.config.v1alpha1.Key;
import io.gingersnapproject.proto.api.config.v1alpha1.KeyFormat;

public class ConfigMapEventHandlerTest {

    static ConfigMapEventHandler cmeh;
    static DynamicRuleManagement drmMock;

    @BeforeAll
    public static void setup() {
        // DynamicRuleManagement drm = new DynamicRuleManagement() {

        // @Override
        // public void addRule(String name, Rule rule) {
        // // TODO Auto-generated method stub

        // }

        // @Override
        // public void removeRule(String name) {
        // // TODO Auto-generated method stub

        // }

        // };
        drmMock = Mockito.mock(DynamicRuleManagement.class);
        cmeh = new ConfigMapEventHandler(drmMock);
    }

    @Test
    public void addRulesTest() {
        var cm = new ConfigMap();
        cm.setData(Map.of("rule1", "value1", "rule2", "value2"));
        cmeh.onAdd(cm);
        verify(drmMock, times(1)).addRule(eq("rule1"), any(Rule.class));
        verify(drmMock, times(1)).addRule(eq("rule2"), any(Rule.class));
    }

    @Test
    public void deleteRulesTest() {
        var cm = new ConfigMap();
        cm.setData(Map.of("rule1", "value1", "rule2", "value2"));
        cmeh.onDelete(cm, true);
        verify(drmMock, times(1)).removeRule(eq("rule1"));
        verify(drmMock, times(1)).removeRule(eq("rule2"));
    }

    @Test
    public void updateRulesTest() {
        var cmOld = new ConfigMap();
        cmOld.setData(Map.of("rule1", "value1", "rule2", "value2"));
        var cmNew = new ConfigMap();
        cmNew.setData(Map.of("rule1", "value1", "rule3", "value3"));
        cmeh.onUpdate(cmOld, cmNew);
        verify(drmMock, times(1)).addRule(eq("rule3"), any(Rule.class));
        verify(drmMock, times(1)).removeRule(eq("rule2"));
    }

    @Test
    public void emptyOldCmTest() {
        var cmOld = new ConfigMap();
        var cmNew = new ConfigMap();
        cmNew.setData(Map.of("rule1", "value1", "rule3", "value3"));
        cmeh.onUpdate(cmOld, cmNew);
        verify(drmMock, times(1)).addRule(eq("rule3"), any(Rule.class));
        verify(drmMock, times(1)).addRule(eq("rule1"), any(Rule.class));
    }

    @Test
    public void emptyNewCmTest() {
        var cmOld = new ConfigMap();
        cmOld.setData(Map.of("rule1", "value1", "rule3", "value3"));
        var cmNew = new ConfigMap();
        cmeh.onUpdate(cmOld, cmNew);
        verify(drmMock, times(1)).removeRule(eq("rule3"));
        verify(drmMock, times(1)).removeRule(eq("rule1"));
    }

    @Test
    public void ruleAdaptorAddTest() throws Exception {
        EagerCacheRuleSpec.Builder eRuleBuilder = EagerCacheRuleSpec.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(JSONMappingTest.eRuleTestCaseJSON, eRuleBuilder);
        var eagerRuleB = eRuleBuilder.build();
        var rule = new EagerCacheRuleSpecAdapterForTest(eagerRuleB);
        var cm = new ConfigMap();
        cm.setData(Map.of("eagerRule", JSONMappingTest.eRuleTestCaseJSON));
        cmeh.onAdd(cm);        
        verify(drmMock, times(1)).addRule(eq("eagerRule"), eq(rule));
    }

    @Test
    public void ruleAdaptorUpdateTest() throws Exception {
        EagerCacheRuleSpec.Builder eRuleBuilderNew = EagerCacheRuleSpec.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(JSONMappingTest.eRuleTestCase2JSON, eRuleBuilderNew);
        var eagerRuleBNew = eRuleBuilderNew.build();
        var ruleNew = new EagerCacheRuleSpecAdapterForTest(eagerRuleBNew);
        var cm = new ConfigMap();
        cm.setData(Map.of("eagerRule", JSONMappingTest.eRuleTestCaseJSON));
        var cm2 = new ConfigMap();
        cm2.setData(Map.of("eagerRuleNew", JSONMappingTest.eRuleTestCase2JSON));
        cmeh.onUpdate(cm,cm2);
        verify(drmMock, times(1)).addRule(eq("eagerRuleNew"), eq(ruleNew));
        verify(drmMock, times(1)).removeRule(eq("eagerRule"));
    }

    @Test
    public void updateSameRuleThrowsTest() throws Exception {
        var cm = new ConfigMap();
        cm.setData(Map.of("eagerRule", JSONMappingTest.eRuleTestCaseJSON));
        var cm2 = new ConfigMap();
        cm2.setData(Map.of("eagerRule", JSONMappingTest.eRuleTestCase2JSON));
        Exception ex = assertThrows(UnsupportedOperationException.class, () ->  cmeh.onUpdate(cm,cm2));
        assertTrue(ex.getMessage().contains(JSONMappingTest.eRuleTestCase2JSON));
        assertTrue(ex.getMessage().contains(JSONMappingTest.eRuleTestCaseJSON));
    }

}

class EagerCacheRuleSpecAdapterForTest  extends EagerCacheRuleSpecAdapter {
    public EagerCacheRuleSpecAdapterForTest(EagerCacheRuleSpec eagerRule) {
        super(eagerRule);
    }
    @Override
    public boolean equals(Object obj) {
       if (obj == this) {
          return true;
       }
       if (!(obj instanceof Rule)) {
          return false;
       }
       var oRule = (Rule)obj;
       var retVal = true;
       retVal &= oRule.connector().schema().equals(connector().schema());
       retVal &= oRule.connector().table().equals(connector().table());
       retVal &= oRule.keyColumns().equals(keyColumns());
       retVal &= oRule.keyType().equals(keyType());
       retVal &= oRule.plainSeparator().equals(plainSeparator());
       retVal &= oRule.valueColumns().equals(valueColumns());
       return retVal;
    }
 
}
