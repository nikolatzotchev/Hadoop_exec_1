package org.example;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChiSquare {
    public static String getCategory(String text) {
        int startIndex = text.indexOf("\"category\": \"");
        if (startIndex == -1) {
            return "";
        }
        startIndex += "\"category\": \"".length();
        int endIndex = text.length() - 2;
        return text.substring(startIndex, endIndex).toLowerCase();
    }
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
        Configuration conf = new Configuration();

        Job preCalcJob = Job.getInstance(conf, "get Total Counts");
        preCalcJob.setJarByClass(ChiSquare.class);
        preCalcJob.setMapperClass(TotalCategory.TotalCategoryMapper.class);
        preCalcJob.setMapOutputKeyClass(Text.class);
        preCalcJob.setMapOutputValueClass(LongWritable.class);
        preCalcJob.setCombinerClass(TotalCategory.CustomReducer.class);
        preCalcJob.setReducerClass(TotalCategory.CustomReducer.class);
        preCalcJob.setOutputKeyClass(Text.class);
        preCalcJob.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(preCalcJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preCalcJob, new Path(args[1]));
        preCalcJob.waitForCompletion(true);

        Job mainJob = Job.getInstance(conf, "general data");
 //       mainJob.addCacheFile(new Path("/user/petko/output/part-r-00000").toUri());
        mainJob.addCacheFile(new Path("/user/e12228456/output1/part-r-00000").toUri());
        mainJob.setJarByClass(ChiSquare.class);
        mainJob.setMapperClass(ChiSquareCalc.WordInCategoryMapper.class);
        mainJob.setMapOutputKeyClass(Text.class);
        mainJob.setMapOutputValueClass(CustomWordWritable.class);

        mainJob.setCombinerClass(ChiSquareCalc.CustomCombiner.class);
        mainJob.setReducerClass(ChiSquareCalc.CustomReducer.class);
        mainJob.setOutputKeyClass(Text.class);
        mainJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(mainJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(mainJob, new Path(args[2]));
        mainJob.waitForCompletion(true);

        Job finalSort = Job.getInstance(conf, "sort");
        finalSort.setJarByClass(ChiSquare.class);
        finalSort.setMapperClass(FinalSort.FinalSortMapper.class);
        finalSort.setReducerClass(FinalSort.FinalSortReducer.class);
        finalSort.setOutputKeyClass(Text.class);
        finalSort.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(finalSort, new Path(args[2]));
        FileOutputFormat.setOutputPath(finalSort, new Path(args[3]));
        finalSort.waitForCompletion(true);
    }
}
