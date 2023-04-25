package org.example;

public class CustomPair implements Comparable<CustomPair> {
    private String key;
    private double value;
    public CustomPair(String key, double value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(CustomPair o) {
        return Double.compare(this.value, o.value);
    }

    public String getKey() {
        return key;
    }

    public double getValue() {
        return value;
    }
}
