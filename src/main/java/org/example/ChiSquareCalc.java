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

/**
 * This is the class containing the mapper, combiner and reducer for the main job, that calculates the chi values.
 */
public class ChiSquareCalc {
    /**
     * The mapper just emits a pair that consists as the unigram as key and the custom writable as value
     * this writable contains the category and number of occurrences (here 1).
     */
    public static class CustomChiMapper extends Mapper<Object, Text, Text, CustomWordWritable> {
        HashSet<String> stopWords = ChiSquare.createSet();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(ChiSquare.getOnlyReviewText(value.toString()), " \t\n\r\f()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/");
            HashSet<String> setOfWords = new HashSet<>();

            // get a set of all words in review
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.length() != 1 && !stopWords.contains(token)) {
                    setOfWords.add(token);
                }
            }

            String cat = ChiSquare.getCategory(value.toString());
            // here we write the pairs for each word in the set
            for (String t : setOfWords) {
                CustomWordWritable customWordWritable = new CustomWordWritable();
                customWordWritable.setCategory(new Text(cat));
                customWordWritable.setCount(new LongWritable(1));
                word.set(t);
                context.write(word, customWordWritable);
            }
        }
    }

    /**
     * This combiner just combines the results of the mapper before being passed to the reducer.
     * I.e. calculates the total number of <word, document> in this mapper
     */
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

    /**
     * This is the reducer class which calculates the chi square values
     */
    public static class CustomReducer
            extends Reducer<Text, CustomWordWritable, Text, Text> {
        Map<String, Long> categoryMap = new HashMap<>();

        HashMap<String, PriorityQueue<CustomPair>> sortMap = new HashMap<>();

        /**
         * Setup reads the cached file from the config and creates the map containing the total count for each category.
         * @param context the context
         * @throws IOException if something happens when reading the file.
         */
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

        /**
         * The main reducer method, which for each <word, dict> calculate the chi square result.
         * @param key
         * @param values
         * @param context
         */
        public void reduce(Text key, Iterable<CustomWordWritable> values,
                           Context context
        ) {
            HashMap<String, Long> mapC = new HashMap<>();

            long sumAllCat = 0;

            for (CustomWordWritable val : values) {
                sumAllCat += val.getCountInt();
                mapC.put(val.getCategoryString(), mapC.getOrDefault(val.getCategoryString(), 0L) + val.getCountInt());
            }
            long N = categoryMap.get("ALL");

            for (String f : mapC.keySet()) {
                long A = mapC.get(f);
                long B = sumAllCat - A;
                long C = categoryMap.get(f) - A;
                long D = N - categoryMap.get(f) - (B);
                float r = (float) ((A * D - B * C) * (A * D - B * C)) / ((A + B) * (A + C) * (B + D) * (C + D));
                // this can be omitted if we only want the ranking
               //  r = N * r;

                if (sortMap.containsKey(f)) {
                    PriorityQueue<CustomPair> p = sortMap.get(f);
                    p.add(new CustomPair(key.toString(), r));
                } else {
                    PriorityQueue<CustomPair> prio = new PriorityQueue<>();
                    prio.add(new CustomPair(key.toString(), r));
                    sortMap.put(f, prio);
                }
                // only keep top 75 results
                if (sortMap.get(f).size() > 75) {
                    sortMap.get(f).poll();
                }
            }
        }

        /**
         * Clean up is used to write the results once all pars for word have been processed.
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
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
