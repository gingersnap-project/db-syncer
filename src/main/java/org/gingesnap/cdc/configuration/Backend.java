package org.gingesnap.cdc.configuration;

import java.net.URI;
import java.util.List;
import java.util.Optional;

public interface Backend {

   URI uri();

   Optional<List<String>> keyColumns();

   Optional<List<String>> columns();
}
