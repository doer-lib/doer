package transitsims;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbConfig;

/** JSON-B with formatting, for the json_data columns. Created on first use. */
final class JsonData {
    static final Jsonb JSONB = JsonbBuilder.create(new JsonbConfig().withFormatting(true));

    private JsonData() {
    }
}
