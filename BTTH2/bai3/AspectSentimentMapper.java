package bai3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AspectSentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text keyOut = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";", -1);
        if (fields.length < 5) return;

        String aspect = fields[3].trim();
        String sentiment = fields[4].trim().toLowerCase();

        if (sentiment.equals("positive") || sentiment.equals("negative")) {
            // Key: aspect_sentiment (e.g. "SERVICE_positive")
            keyOut.set(aspect + "\t" + sentiment);
            context.write(keyOut, one);
        }
    }
}
