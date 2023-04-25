package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * This is the class which contains the mapper and reducer for the final job of sorting.
 */
public class FinalSort {

    /**
     * This is the mapper that just emits each line.
     */
    public static class FinalSortMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyText = new Text();
        private final Text valueText = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] inp = value.toString().split ("\t");
            keyText.set(inp[0]);
            valueText.set(inp[1]);
            context.write(keyText, valueText);
        }
    }

    /**
     * The reducer just gets each line and add its values to the set that will be used to display the whole dictionary
     */
    public static class FinalSortReducer
            extends Reducer<Text, Text, Text, Text> {
        Set<String> set = new TreeSet<>();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text val : values) {
                for (String v : val.toString().split("\\s+")) {
                    String word = v.split(":")[0];
                    stringBuilder.insert(0, " ");
                    stringBuilder.insert(0, v);
                    set.add(word);
                }
                context.write(key, new Text(stringBuilder.toString()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder b = new StringBuilder();
            for (String v : set) {
                b.append(v).append(" ");
            }
            context.write(new Text(b.toString()), new Text(""));
        }
    }

}
