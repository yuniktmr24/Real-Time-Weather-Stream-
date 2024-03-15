package org.lossy;

import java.util.ArrayList;
import java.util.List;

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
        for (BucketElement el: bucketList) {
            sb.append(el.toString());
            sb.append("\n");
        }
        return sb.toString();
    }
}
