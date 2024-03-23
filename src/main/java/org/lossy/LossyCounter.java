package org.lossy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LossyCounter {
    private String descriptor;
    private double THRESHOLD_EPSILON = 0.1; //default was 0.2

    private int bucketSize;
    private List<BucketElement> bucketList = new ArrayList<>();

    private int elementCounter;

    public LossyCounter(double threshold) {
        THRESHOLD_EPSILON = threshold;
        bucketSize = (int) Math.ceil(1/THRESHOLD_EPSILON);
    }
    public LossyCounter() {
        bucketSize = (int) Math.ceil(1/THRESHOLD_EPSILON);
    }

    public LossyCounter(String descriptor) {
        this.descriptor = descriptor;
        bucketSize = (int) Math.ceil(1/THRESHOLD_EPSILON);
    }

    public void accept(Object input) {
        boolean containsInput = bucketList.stream().anyMatch(el -> el.getContent().equals(input));

        if (containsInput) {
            BucketElement existingEl = bucketList.stream()
                    .filter(element -> element.getContent().equals(input))
                    .findFirst().get();
            existingEl.incrementFrequency();
        }
        else {
            BucketElement newEl = new BucketElement(input, elementCounter / bucketSize);
            insert(bucketList, newEl);
        }
        elementCounter++;
        if (elementCounter != 0 && (elementCounter % bucketSize == 0)) {
            delete(bucketList, elementCounter / bucketSize);
        }
    }

    private void insert(List <BucketElement> targetList, BucketElement el) {
        targetList.add(el);
    }

    private void delete(List <BucketElement> targetList, int bucketIndex) {
        targetList.removeIf(el -> (el.getFrequency() + el.getDelta()) <= bucketIndex);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.descriptor).append("\n");
        if (!bucketList.isEmpty()) {
            List<BucketElement> sorted = bucketList.stream()
                    .sorted(Comparator.comparingInt(BucketElement::getFrequency).reversed())
                    .collect(Collectors.toList());
            int idx = 0;
            for (BucketElement el: sorted) {
                sb.append(el.toString());
                sb.append("\n");
                idx++;
                if (idx == 5) break;
            }
        }
        else {
            sb.append("No states available");
        }

        return sb.toString();
    }

    private void normalizeBucketElements (Map<String, Long> normalizationMap) {
        for (BucketElement el: bucketList) {
            String content = (String) el.getContent();

            int totalCount = normalizationMap.getOrDefault(content, 1L).intValue();

            if (totalCount == 0) {
                //won't happen. but well let's make the code failsafe
                continue;
            }
            double normalizedFreq = (double) el.getFrequency() / totalCount;

            el.setNormalizedFrequency(normalizedFreq);
        }
    }

    public String toNormalizedString(Map<String, Long> normalizationMap) {
        normalizeBucketElements(normalizationMap);
        StringBuilder sb = new StringBuilder();
        if (!bucketList.isEmpty()) {
            List<BucketElement> sorted = bucketList.stream()
                    .sorted(Comparator.comparingDouble(BucketElement::getNormalizedFrequency).reversed())
                    .collect(Collectors.toList());
            int idx = 0;
            for (BucketElement el: sorted) {
                sb.append(el.toNormalizedString());
                sb.append("\n");
                idx++;
                if (idx == 5) break;
            }
        }
        else {
            sb.append("No states available");
        }

        return sb.toString();
    }

    public String getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(String descriptor) {
        this.descriptor = descriptor;
    }
}
