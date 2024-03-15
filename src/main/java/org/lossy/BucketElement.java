package org.lossy;

public class BucketElement {
    private Object content;

    private int frequency;

    private int delta;

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
}
