package org.lossy;

public class BucketElement {
    private Object content;

    private int frequency;

    private int delta;

    private double normalizedFrequency;

    public BucketElement(Object content, int delta) {
        this.content = content;
        this.delta = delta;
        this.frequency++;
    }

    public Object getContent() {
        return content;
    }

    public void incrementFrequency () {
        this.frequency++;
    }

    public int getFrequency() {
        return frequency;
    }

    public int getDelta() {
        return delta;
    }

    @Override
    public String toString() {
        return "BucketElement{" +
                "content=" + content +
                ", frequency=" + frequency +
                ", delta=" + delta +
                '}';
    }


    public String toNormalizedString() {
        return "BucketElement{" +
                "content=" + content +
                ", frequency=" + frequency +
                ", delta=" + delta +
                ", normalizedFrequency=" + normalizedFrequency +
                '}';
    }

    public void setNormalizedFrequency(double normalizedFrequency) {
        this.normalizedFrequency = normalizedFrequency;
    }

    public double getNormalizedFrequency() {
        return normalizedFrequency;
    }
}
