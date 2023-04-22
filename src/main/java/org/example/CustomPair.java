package org.example;

public class CustomPair implements Comparable<CustomPair> {
    private String key;
    private float value;
    public CustomPair(String key, float value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(CustomPair o) {
        return Float.compare(this.value, o.value);
    }

    public String getKey() {
        return key;
    }

    public float getValue() {
        return value;
    }
}
