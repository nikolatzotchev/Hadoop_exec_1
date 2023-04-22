package org.example;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class ChiSquareCalc {
    public static class WordInCategoryMapper extends Mapper<Object, Text, Text, CustomWordWritable> {
        HashSet<String> stopWords = ChiSquare.createSet();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(ChiSquare.getOnlyReviewText(value.toString()), " \t\n\r\f()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/");
            HashSet<String> setOfWords = new HashSet<>();

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.length() != 1 && !stopWords.contains(token)) {
                    setOfWords.add(token);
                }
            }

            String cat = ChiSquare.getCategory(value.toString());

            for (String t : setOfWords) {
                CustomWordWritable customWordWritable = new CustomWordWritable();
                customWordWritable.setCategory(new Text(cat));
                customWordWritable.setCount(new LongWritable(1));
                word.set(t);
                context.write(word, customWordWritable);
            }
        }
    }
    public static class CustomCombiner extends Reducer<Text, CustomWordWritable, Text, CustomWordWritable> {

        public void reduce(Text key, Iterable<CustomWordWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            Map<String, Long> categoryCountMap = new HashMap<>();

            // compute the sum of counts and the counts per category
            for (CustomWordWritable value : values) {
                sum += value.getCountInt();
                String category = value.getCategoryString();
                categoryCountMap.put(category, categoryCountMap.getOrDefault(category, 0L) + value.getCountInt());
            }

            // emit the total count and the counts per category for this word
            CustomWordWritable outValue = new CustomWordWritable();
            outValue.setCount(new LongWritable(sum));
            for (String category : categoryCountMap.keySet()) {
                outValue.setCategory(new Text(category));
                outValue.setCount(new LongWritable(categoryCountMap.get(category)));
                context.write(key, outValue);
            }
        }
    }
    public static class CustomReducer
            extends Reducer<Text, CustomWordWritable, Text, Text> {
        Map<String, Long> categoryMap = new HashMap<>();

        HashMap<String, PriorityQueue<CustomPair>> sortMap = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException {
            URI fileURI = context.getCacheFiles()[0];

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream inputStream = fs.open(new Path(fileURI.toString()));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\s+");
                String key = tokens[0];
                Long value = Long.parseLong(tokens[1]);
                categoryMap.put(key, value);
            }
            reader.close();
            inputStream.close();
        }

        public void reduce(Text key, Iterable<CustomWordWritable> values,
                           Context context
        ) {
            HashMap<String, Long> mapC = new HashMap<>();
            // A
            long sumAllCat = 0;

            for (CustomWordWritable val : values) {
                sumAllCat += val.getCountInt();
                mapC.put(val.getCategoryString(), mapC.getOrDefault(val.getCategoryString(), 0L) + val.getCountInt());
            }

            //context.write(key, new IntWritable(sumAllCat));
            long A = sumAllCat;
            for (String f : mapC.keySet()) {
                long B = sumAllCat - mapC.get(f);
                long C = categoryMap.get(f) - mapC.get(f);
                long D = categoryMap.get("ALL") - categoryMap.get(f) - (sumAllCat - mapC.get(f));
                float r = (float) ((A * D - B * C) * (A * D - B * C)) / ((A + B) * (A + C) * (B + D) * (C + D));

                if (sortMap.containsKey(f)) {
                    PriorityQueue<CustomPair> p = sortMap.get(f);
                    p.add(new CustomPair(key.toString(), r));
                } else {
                    PriorityQueue<CustomPair> prio = new PriorityQueue<>();
                    prio.add(new CustomPair(key.toString(), r));
                    sortMap.put(f, prio);
                }
                if (sortMap.get(f).size() > 75) {
                    sortMap.get(f).poll();
                }
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, PriorityQueue<CustomPair>> entry : sortMap.entrySet()) {
                StringBuilder result = new StringBuilder();
                PriorityQueue<CustomPair> q = entry.getValue();
                while (!q.isEmpty()) {
                    CustomPair c = q.poll();
                    result.append(c.getKey()).append(":").append(c.getValue()).append(" ");
                }
                context.write(new Text(entry.getKey()), new Text(result.toString()));
            }
        }
    }
}
