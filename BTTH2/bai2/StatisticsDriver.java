package bai2;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StatisticsDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: StatisticsDriver <input> <output> <stopwords_path> <job_type>");
            System.err.println("  job_type: wordfreq | category | aspect");
            System.exit(1);
        }

        String jobType = args[3];

        Configuration conf = new Configuration();
        conf.set("job.type", jobType);

        if (jobType.equals("wordfreq")) {
            conf.setInt("word.threshold", 500);
        }

        Job job = Job.getInstance(conf, "Bai2 - Statistics (" + jobType + ")");

        job.setJarByClass(StatisticsDriver.class);
        job.setMapperClass(StatisticsMapper.class);
        job.setReducerClass(StatisticsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new URI(args[2] + "#stopwords.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
