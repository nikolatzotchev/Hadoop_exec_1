package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class FinalSort {
    public static class FinalSortMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyText = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] inp = value.toString().split ("\t");
            context.write(new Text(inp[0]), new Text(inp[1]));
        }
    }

    public static class FinalSortReducer
            extends Reducer<Text, Text, Text, Text> {
        Set<String> set = new TreeSet<>();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {
                for (String v : val.toString().split("\\s+")) {
                    String word = v.split(":")[0];
                    set.add(word);
                }
                context.write(key, val);
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
