package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TotalCategory {

    /**
     * This is the mapper for the job counting the documents in each category.
     * It works the same as word count mapper, but it also writes a key "ALL" used to count all the
     * documents.
     */
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

    /**
     * This is the reducer for counting the total documents in each category
     * it is the same as the reducer for the basic example of word count.
     */
    public static class TotalCategoryReducer
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
