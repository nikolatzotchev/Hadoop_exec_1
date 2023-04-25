package org.example;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChiSquare {
    /**
     * Extracts the category from the String of one document
     * @param text the String containing the document
     * @return only the string of the category
     */
    public static String getCategory(String text) {
        int startIndex = text.indexOf("\"category\": \"");
        if (startIndex == -1) {
            return "";
        }
        startIndex += "\"category\": \"".length();
        int endIndex = text.length() - 2;
        return text.substring(startIndex, endIndex).toLowerCase();
    }

    /**
     * Extract the reviewText field from a document.
     * @param text the document as string.
     * @return only the reviewText field string.
     */
    public static String getOnlyReviewText(String text) {
        int startIndex = text.indexOf("\"reviewText\": \"");
        if (startIndex == -1) {
            return "";
        }
        startIndex += 15;
        int endIndex = text.indexOf("\", \"overall\":");
        if (endIndex == -1) {
            endIndex = text.lastIndexOf("\"}");
        }
        return text.substring(startIndex, endIndex).toLowerCase();
    }

    /**
     * Parse the stopwords from the file provided in the assignment.
     * @return a set containing all stop words
     */
    protected static HashSet<String> createSet() {
        HashSet<String> stopwords = new HashSet<>();
        try {
            File file = new File(ChiSquare.class.getResource("/stopwords.txt").getPath());
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                stopwords.add(line);
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return stopwords;
    }



    public static void main(String[] args) throws Exception {
        Path customPathOutputJob1 = new Path("/user/e12228456/output1input2");
        Path customPathOutputJob2 = new Path("/user/e12228456/output2input3");
        Configuration conf = new Configuration();

        /*
          This is the first job that will calculate the sum of all documents per category.
          It will also calculate the total sum of all documents.
         */
        Job preCalcJob = Job.getInstance(conf, "get Total Counts");
        preCalcJob.setJarByClass(ChiSquare.class);
        preCalcJob.setMapperClass(TotalCategory.TotalCategoryMapper.class);
        preCalcJob.setMapOutputKeyClass(Text.class);
        preCalcJob.setMapOutputValueClass(LongWritable.class);
        preCalcJob.setCombinerClass(TotalCategory.TotalCategoryReducer.class);
        preCalcJob.setReducerClass(TotalCategory.TotalCategoryReducer.class);
        preCalcJob.setOutputKeyClass(Text.class);
        preCalcJob.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(preCalcJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preCalcJob, customPathOutputJob1);
        preCalcJob.waitForCompletion(true);
        /*
          This is the main job that calculates the chi values of all <word, category> pairs.
          It gets passed the whole data as well as a cache file containing the results of job1.
         */
        Job mainJob = Job.getInstance(conf, "general data");
        mainJob.addCacheFile(new Path(customPathOutputJob1 + "/part-r-00000").toUri());
//        mainJob.addCacheFile(new Path("/user/e12228456/output1/part-r-00000").toUri());
        mainJob.setJarByClass(ChiSquare.class);
        mainJob.setMapperClass(ChiSquareCalc.CustomChiMapper.class);
        mainJob.setMapOutputKeyClass(Text.class);
        mainJob.setMapOutputValueClass(CustomWordWritable.class);
        mainJob.setCombinerClass(ChiSquareCalc.CustomCombiner.class);
        mainJob.setReducerClass(ChiSquareCalc.CustomReducer.class);
        mainJob.setOutputKeyClass(Text.class);
        mainJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(mainJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(mainJob, customPathOutputJob2);
        mainJob.waitForCompletion(true);

        /*
          This is the final job that just sorts the values in the order required
          and also appends a line containing the whole dictionary.
         */
        Job finalSort = Job.getInstance(conf, "sort");
        finalSort.setJarByClass(ChiSquare.class);
        finalSort.setMapperClass(FinalSort.FinalSortMapper.class);
        finalSort.setReducerClass(FinalSort.FinalSortReducer.class);
        finalSort.setOutputKeyClass(Text.class);
        finalSort.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(finalSort, customPathOutputJob2);
        FileOutputFormat.setOutputPath(finalSort, new Path(args[1]));
        finalSort.waitForCompletion(true);
    }
}
