package bai1;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MovieRatingDriver <input_ratings> <output> <movies_cache>");
            System.err.println("  input_ratings: HDFS path to ratings files");
            System.err.println("  output: HDFS output path");
            System.err.println("  movies_cache: HDFS path to movies.txt");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai1 - Movie Average Rating");

        job.setJarByClass(MovieRatingDriver.class);
        job.setMapperClass(MovieRatingMapper.class);
        job.setReducerClass(MovieRatingReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add movies.txt to Distributed Cache
        job.addCacheFile(new URI(args[2] + "#movies.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
