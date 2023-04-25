package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWordWritable implements Writable {
    private LongWritable count;
    private Text category;
    public CustomWordWritable() {
        this.count = new LongWritable( 0);
        this.category = new Text();
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.count.write(dataOutput);
        this.category.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count.readFields(dataInput);
        this.category.readFields(dataInput);
    }

    public LongWritable getCount() {
        return count;
    }

    public long getCountInt() {
        return this.count.get();
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }

    public Text getCategory() {
        return category;
    }

    public String getCategoryString() {
        return category.toString();
    }

    public void setCategory(Text category) {
        this.category = category;
    }
}