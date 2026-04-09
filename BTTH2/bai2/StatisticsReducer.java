package bai2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    private String jobType;
    private int threshold;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        jobType = context.getConfiguration().get("job.type", "wordfreq");
        threshold = context.getConfiguration().getInt("word.threshold", 0);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }


        if (jobType.equals("wordfreq") && threshold > 0) {
            if (sum > threshold) {
                result.set(sum);
                context.write(key, result);
            }
        } else {
            result.set(sum);
            context.write(key, result);
        }
    }
}
