package org.requests;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.domain.DomainUtils;
import org.domain.ZipCodeData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WeatherAPIRequest {
    private static final String API_KEY = "7b1c4bdeac5e4a63afe164157242703";


    //private static final ExecutorService executorService = Executors.newCachedThreadPool();;

    private static final ExecutorService executorService = Executors.newFixedThreadPool(32);;

    private static final ExecutorService fastExecutorService = Executors.newCachedThreadPool();
    private static final String CURRENT_WEATHER_EP = "/current.json";
    private static final String FORECAST_WEATHER_EP = "/forecast.json";

    // Define the base URI of the endpoint
    private static final String baseUrl = "http://api.weatherapi.com/v1";

    public static void main (String [] args) throws IOException, InterruptedException {

        //sendCurrentWeatherRequest(1);
        Map <String, Long> stateFreqMap = DomainUtils.getStateCounts("src/main/resources/uszips.csv");

        List <ZipCodeData> zip = readZipCodes("src/main/resources/uszips_min.csv");

        for (ZipCodeData z: zip) {
            //sendSingleWeatherRequest(z);
            //sendForecastWeatherRequest(z);
            testForecastAPI(z);
        }
    }


    public static CompletableFuture<WeatherResponse> sendSingleWeatherRequestAsync(ZipCodeData data) {
        CompletableFuture<WeatherResponse> future = new CompletableFuture<>();
        //Executors.newFixedThreadPool(10).submit(() -> {
        fastExecutorService.submit(() -> {
            String ep = baseUrl + CURRENT_WEATHER_EP;
            String encodedAPIKey = null;
            try {
                encodedAPIKey = URLEncoder.encode(API_KEY, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            //encodedAPIKey = URLEncoder.encode(API_KEY, StandardCharsets.UTF_8);
            String urlWithParams = ep + "?key=" + encodedAPIKey + "&q=" + data.getZip() + "&aqi=yes";

            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(5000) // Set the socket timeout (read timeout)
                    .setConnectTimeout(5000) // Set the connection timeout
                    .build();

            //try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                try (CloseableHttpClient httpClient = HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        .build()) {
                HttpGet request = new HttpGet(urlWithParams);
                request.addHeader("Content-Type", "application/json");

                // Execute HTTP request
                String responseBody = httpClient.execute(request, httpResponse ->
                        EntityUtils.toString(httpResponse.getEntity()));

                // Parse response
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(responseBody);
                JsonNode locationNode = rootNode.get("location");
                String locationName = locationNode.get("name").asText();
                String stateName = locationNode.get("region").asText();
                JsonNode currentWeatherNode = rootNode.get("current");
                int aqi = currentWeatherNode.get("air_quality").get("us-epa-index").asInt();
                int cloud = currentWeatherNode.get("cloud").asInt();

                // Create WeatherResponse object
                WeatherResponse res = new WeatherResponse(data.getZip(), locationName, stateName, aqi);
                res.setCloudCover(cloud);

                // Complete the future
                future.complete(res);
            } catch (Exception e) {
                //do nothing
                future.completeExceptionally(e);
            }
        });

        return future;
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

    public static List <WeatherResponse> testForecastAPI (ZipCodeData data) {
        String ep = baseUrl + FORECAST_WEATHER_EP;
        String encodedAPIKey = null;
        try {
            encodedAPIKey = URLEncoder.encode(API_KEY, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String urlWithParams = ep + "?key=" + encodedAPIKey + "&q=" + data.getZip() + "&days=3";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(urlWithParams);
            request.addHeader("Content-Type", "application/json");

            // Execute HTTP request
            String responseBody = httpClient.execute(request, httpResponse ->
                    EntityUtils.toString(httpResponse.getEntity()));

            // Parse response
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(responseBody);

            JsonNode locationNode = rootNode.get("location");
            String locationName = locationNode.get("name").asText();
            String stateName = locationNode.get("region").asText();

            JsonNode forecastDays = rootNode.path("forecast").path("forecastday");

            List<Integer> avgCloudValues = new ArrayList<>();
            List<Integer> cloudValues = new ArrayList<>();
            List <WeatherResponse> resList = new ArrayList<>();


            for (JsonNode forecastDay : forecastDays) {
                WeatherResponse res = new WeatherResponse(data.getZip(), locationName, stateName, 0);
                int totalCloud = 0;
                JsonNode hours = forecastDay.path("hour");

                for (JsonNode hour : hours) {
                    totalCloud += hour.path("cloud").asInt();
                }

                cloudValues.add(totalCloud);
                int cloudMean = totalCloud / 24;
                avgCloudValues.add(cloudMean);

                res.setCloudCover(cloudMean);

                resList.add(res);
            }

            return resList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CompletableFuture<List <WeatherResponse>> sendWeatherForecastRequestAsync(ZipCodeData data) {
        CompletableFuture<List <WeatherResponse>> future = new CompletableFuture<>();
        //Executors.newFixedThreadPool(10).submit(() -> {
        executorService.submit(() -> {
            String ep = baseUrl + FORECAST_WEATHER_EP;
            String encodedAPIKey = null;
            try {
                encodedAPIKey = URLEncoder.encode(API_KEY, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            String urlWithParams = ep + "?key=" + encodedAPIKey + "&q=" + data.getZip() + "&days=3";

            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpGet request = new HttpGet(urlWithParams);
                request.addHeader("Content-Type", "application/json");

                // Execute HTTP request
                String responseBody = httpClient.execute(request, httpResponse ->
                        EntityUtils.toString(httpResponse.getEntity()));

                // Parse response
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(responseBody);

                JsonNode locationNode = rootNode.get("location");
                String locationName = locationNode.get("name").asText();
                String stateName = locationNode.get("region").asText();

                JsonNode forecastDays = rootNode.path("forecast").path("forecastday");

                List<Integer> avgCloudValues = new ArrayList<>();
                List<Integer> cloudValues = new ArrayList<>();
                List <WeatherResponse> resList = new ArrayList<>();


                for (JsonNode forecastDay : forecastDays) {
                    WeatherResponse res = new WeatherResponse(data.getZip(), locationName, stateName, 0);
                    int totalCloud = 0;
                    JsonNode hours = forecastDay.path("hour");

                    for (JsonNode hour : hours) {
                        totalCloud += hour.path("cloud").asInt();
                    }

                    cloudValues.add(totalCloud);
                    int cloudMean = totalCloud / 24;
                    avgCloudValues.add(cloudMean);

                    res.setCloudCover(cloudMean);

                    resList.add(res);
                }

                future.complete(resList);
            } catch (Exception e) {
                //do nothing
                future.completeExceptionally(e);
            }
        });
        return future;
    }

/*
     @Deprecated
    public static List <WeatherResponse> sendCurrentWeatherRequest (int batchSize) {
        return sendCurrentWeatherRequest(baseUrl, batchSize);
    }

    @Deprecated
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
            int cloud = currentWeatherNode.get("cloud").asInt();

            // Create WeatherResponse object and add it to the list
            weatherResponse = new WeatherResponse(data.getZip(), locationName, stateName, aqi);
            weatherResponse.setCloudCover(cloud);

        } catch (Exception e) {
            //move on instead of throwing error and polluting the log
            //throw new RuntimeException(e);
        }
        return weatherResponse;
    }

    @Deprecated
    private static List <WeatherResponse> sendCurrentWeatherRequest (String baseUrl, int batchSize) {
        List<WeatherResponse> res = new ArrayList<>();
        List <ZipCodeData> zip = readZipCodes("src/main/resources/uszips_min.csv");

        List<List<ZipCodeData>> batches = DomainUtils.splitIntoBatches(zip, batchSize);

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

    @Deprecated
    public static void sendForecastWeatherRequest (ZipCodeData data) throws IOException, InterruptedException {
        String ep = baseUrl + FORECAST_WEATHER_EP;
        // Encode the key parameters
        String encodedAPIKey = URLEncoder.encode(API_KEY, StandardCharsets.UTF_8);

        // Construct the complete URL with query parameters
        String urlWithParams = ep + "?key=" + encodedAPIKey + "&q=" + data.getZip() + "&days=3";

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
            JsonNode rootNode = mapper.readTree(res);

            JsonNode forecastDays = rootNode.path("forecast").path("forecastday");

            List<Integer> avgCloudValues = new ArrayList<>();
            List<Integer> cloudValues = new ArrayList<>();

            for (JsonNode forecastDay : forecastDays) {
                int totalCloud = 0;
                JsonNode hours = forecastDay.path("hour");

                for (JsonNode hour : hours) {
                    totalCloud += hour.path("cloud").asInt();
                }

                cloudValues.add(totalCloud);
                avgCloudValues.add(totalCloud / 24);
            }

            System.out.println("Total cloud values for each day: " + avgCloudValues);
        }

         catch (Exception e) {
            //move on instead of throwing error and polluting the log
            //throw new RuntimeException(e);
        }
    }
    */
}

