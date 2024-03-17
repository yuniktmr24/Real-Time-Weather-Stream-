package org.domain;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DomainUtils {
    public static <T> List<List<T>> splitIntoBatches(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            batches.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return batches;
    }

    public static Map <String, Long> getStateCounts (String filePath) throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            Map<String, Long> stateCounts = lines.skip(1)
                    .map(line -> line.split(","))
                    .collect(Collectors.groupingBy(
                            parts -> parts[3], // State is at index 3
                            Collectors.counting()
                    ));
            stateCounts.forEach((state, count) -> System.out.println(state + ": " + count));
            return stateCounts;
            // Print the state counts
        }
    }
}
