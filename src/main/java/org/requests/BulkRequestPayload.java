package org.requests;

import java.util.HashMap;
import java.util.Map;

public class BulkRequestPayload {
    private Map<String, String> queryMap = new HashMap<>();

    public BulkRequestPayload() {
    }

    public void appendQuery(String query, String desc) {
        queryMap.put(query, desc);
    }

    public String constructBulkPayload() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{\n");
        stringBuilder.append("\t\"locations\": [\n");

        // Iterate over the queryMap entries
        int idx = 0;
        for (Map.Entry<String, String> entry : queryMap.entrySet()) {
            stringBuilder.append("\t\t{\n");
            stringBuilder.append("\t\t\t\"q\": \"" + entry.getKey() + "\",\n");
            stringBuilder.append("\t\t\t\"custom_id\": \"" + entry.getValue() + "\"\n");
            stringBuilder.append("\t\t}");

            // Add comma if it's not the last entry
            if (idx != queryMap.size() - 1) {
                stringBuilder.append(",");
            }
            stringBuilder.append("\n");
            idx++;
        }

        stringBuilder.append("\t]\n");
        stringBuilder.append("}");

        return stringBuilder.toString();
    }
}
