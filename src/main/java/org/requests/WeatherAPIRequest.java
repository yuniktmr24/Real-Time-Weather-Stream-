package org.requests;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.domain.Utils;
import org.domain.ZipCodeData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class WeatherAPIRequest {
    private static final String API_KEY = "8f9666fbcaf642b8911204604240703";

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final String CURRENT_WEATHER_EP = "/current.json";

    // Define the base URI of the endpoint
    private static final String baseUrl = "http://api.weatherapi.com/v1";

    public static void main (String [] args) throws IOException, InterruptedException {

        //sendCurrentWeatherRequest(1);

        List <ZipCodeData> zip = readZipCodes("src/main/resources/uszips_min.csv");

        for (ZipCodeData z: zip) {
            sendSingleWeatherRequest(z);
        }
    }

    public static List <WeatherResponse> sendCurrentWeatherRequest (int batchSize) {
        return sendCurrentWeatherRequest(baseUrl, batchSize);
    }

    public static CompletableFuture<WeatherResponse> sendSingleWeatherRequestAsync(ZipCodeData data) {
        String ep = baseUrl + CURRENT_WEATHER_EP;
        String encodedAPIKey = URLEncoder.encode(API_KEY, StandardCharsets.UTF_8);
        String urlWithParams = ep + "?key=" + encodedAPIKey + "&q=" + data.getZip() + "&aqi=yes";

        URI uri = URI.create(urlWithParams);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .header("Content-Type", "application/json")
                .GET()
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode rootNode = mapper.readTree(response.body());
                        JsonNode locationNode = rootNode.get("location");
                        String locationName = locationNode.get("name").asText();
                        String stateName = locationNode.get("region").asText();
                        JsonNode currentWeatherNode = rootNode.get("current");
                        int aqi = currentWeatherNode.get("air_quality").get("us-epa-index").asInt();
                        int cloud = currentWeatherNode.get("cloud").asInt();

                       // ((CloseableHttpResponse) response).close();
                        WeatherResponse res = new WeatherResponse(data.getZip(), locationName, stateName, aqi);
                        res.setCloudCover(cloud);

                        return res;
                    } catch (IOException | NullPointerException e) {
                        //e.printStackTrace();
                        return null;
                    }
                });
    }


    public static WeatherResponse sendSingleWeatherRequest (ZipCodeData data) throws IOException, InterruptedException {
        String ep = baseUrl + CURRENT_WEATHER_EP;
        // Encode the key parameters
        String encodedAPIKey = URLEncoder.encode(API_KEY, StandardCharsets.UTF_8);

        // Construct the complete URL with query parameters
        String urlWithParams = ep + "?key=" + encodedAPIKey + "&q=" + data.getZip() + "&aqi=yes";

        URI uri = URI.create(urlWithParams);
        HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(uri)
                .header("Content-Type", "application/json")
                //.POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .GET() // This specifies that it's a GET request
                .build();

        WeatherResponse weatherResponse = null;
        try {
            // Send the request and capture the response
            java.net.http.HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());


            String res = response.body();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(response.body());
            JsonNode locationNode = rootNode.get("location");
            String locationName = locationNode.get("name").asText();
            String stateName = locationNode.get("region").asText();

            JsonNode currentWeatherNode = rootNode.get("current");
            int aqi = currentWeatherNode.get("air_quality").get("us-epa-index").asInt();

            // Create WeatherResponse object and add it to the list
            weatherResponse = new WeatherResponse(data.getZip(), locationName, stateName, aqi);

        } catch (Exception e) {
            //move on instead of throwing error and polluting the log
            //throw new RuntimeException(e);
        }
        return weatherResponse;
    }

    private static List <WeatherResponse> sendCurrentWeatherRequest (String baseUrl, int batchSize) {
        List<WeatherResponse> res = new ArrayList<>();
        List <ZipCodeData> zip = readZipCodes("src/main/resources/uszips_min.csv");

        List<List<ZipCodeData>> batches = Utils.splitIntoBatches(zip, batchSize);

        baseUrl = baseUrl + CURRENT_WEATHER_EP;
        // Encode the key parameters
        String encodedAPIKey = URLEncoder.encode(API_KEY, StandardCharsets.UTF_8);

        // Construct the complete URL with query parameters
        String urlWithParams = baseUrl + "?key=" + encodedAPIKey + "&q=bulk" + "&aqi=yes";

        // Create a URI object
        long start = System.nanoTime();
        URI uri = URI.create(urlWithParams);

        for (List<ZipCodeData> batch : batches) {

            BulkRequestPayload payload = new BulkRequestPayload();

            for (ZipCodeData data : batch) {
                payload.appendQuery(data.getZip(), data.getCity());
            }
            //        //read this from provided csv
            //        payload.appendQuery("80521", "Foco");
            //        payload.appendQuery("80304", "Boulder");
            //        payload.appendQuery("38655", "MS");

            String requestBody = payload.constructBulkPayload();

            //System.out.println(requestBody);

            // Create a GET request
            HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .uri(uri)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    //.GET() // This specifies that it's a GET request
                    .build();

            try {
                // Send the request and capture the response
                java.net.http.HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

                // Print the status code and response body
                //System.out.println("Response Code: " + response.statusCode());
                //System.out.println("Response Body: " + response.body());

                //todo extract top 5 highest us-epa-index
                if (res.isEmpty()) {
                    res = ResponseParser.parseBulkResponse(response.body());
                }
                else {
                    for (WeatherResponse res2: ResponseParser.parseBulkResponse(response.body())) {
                        res.add(res2);
                    }
                }

                //res.forEach(i -> System.out.println(i.toString()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long end = System.nanoTime();
        long executionTime = end - start;
        long numSecs = executionTime/1_000_000_000;
        System.out.println("Done in : "+ numSecs + " secs");
        return res;
    }

    public static List<ZipCodeData> readZipCodes(String filePath) {
        List<ZipCodeData> zipCodes = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            // Skip the header line
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 4) {
                    String zip = parts[0];
                    String city = parts[1];
                    String stateId = parts[2];
                    String stateName = parts[3];
                    zipCodes.add(new ZipCodeData(zip, city, stateId, stateName));
                } else {
                    System.err.println("Invalid line in CSV: " + line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return zipCodes;
    }
}

