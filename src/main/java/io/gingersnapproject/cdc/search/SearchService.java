package io.gingersnapproject.cdc.search;

import java.net.URI;

import io.gingersnapproject.cdc.SearchBackend;
import io.gingersnapproject.cdc.translation.JsonTranslator;

public interface SearchService {

   SearchBackend backendForURI(URI uri, JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator);

}
