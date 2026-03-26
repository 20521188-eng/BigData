package bai2;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenreRatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        double avg = sum / count;

        String result = "Avg: " + String.format("%.2f", avg) + ", Count: " + count;
        context.write(key, new Text(result));
    }
}
