package org.requests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class ResponseParser {
    public static List <AQIResponse> parseBulkResponse(String responseBody) throws Exception {
        List<AQIResponse> aqi = new ArrayList<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(responseBody);

            // Extract the us-epa-index
            JsonNode bulkNode = rootNode.get("bulk");
            if (bulkNode.isArray()) {
                for (JsonNode node : bulkNode) {
                    JsonNode queryNode = node.get("query");
                    if (queryNode != null) {
                        JsonNode q = queryNode.get("q");
                        JsonNode current = queryNode.get("current");
                        JsonNode location = queryNode.get("location");
                        String locationName = "";
                        String state = "";
                        if (location != null) {
                            locationName = location.get("name").asText();
                            state = location.get("region").asText();
                        }
                        if (current != null) {
                            JsonNode airQuality = current.get("air_quality");
                            if (airQuality != null) {
                                JsonNode usEpaIndex = airQuality.get("us-epa-index");
                                if (usEpaIndex != null) {
                                    int epaIndex = usEpaIndex.asInt();
                                    String zip = q.asText();
                                    aqi.add(new AQIResponse(zip, locationName, state, epaIndex));
                                }
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex) {
            //move on
            //ex.printStackTrace();
        }
        return aqi;
    }
}