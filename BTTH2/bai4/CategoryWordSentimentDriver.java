package bai4;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryWordSentimentDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: CategoryWordSentimentDriver <input> <output> <stopwords_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai4 - Top Words by Category & Sentiment");

        job.setJarByClass(CategoryWordSentimentDriver.class);
        job.setMapperClass(CategoryWordSentimentMapper.class);
        job.setReducerClass(CategoryWordSentimentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[2] + "#stopwords.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
