package io.gingersnapproject.cdc.translation;

import java.util.function.Function;

import org.infinispan.commons.dataconversion.internal.Json;

public interface JsonTranslator<E> extends Function<Json, E> {
}
