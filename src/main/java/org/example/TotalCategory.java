package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TotalCategory {

    public static class TotalCategoryMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final Text keyText = new Text();
        private final LongWritable longWritable = new LongWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cat = ChiSquare.getCategory(value.toString());
            keyText.set(cat);
            context.write(keyText, longWritable);
            keyText.set("ALL");
            context.write(keyText, longWritable);
        }
    }

    public static class CustomReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
