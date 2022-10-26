package org.gingesnap.cdc.configuration;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigDocMapKey;
import io.quarkus.runtime.annotations.ConfigGroup;
import io.quarkus.runtime.annotations.ConfigItem;
import io.smallrye.config.WithName;

public interface Backend {

   String uri();

   Optional<List<String>> columns();
}
