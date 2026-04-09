package bai3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AspectSentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    private Map<String, Integer> positiveCount = new HashMap<String, Integer>();
    private Map<String, Integer> negativeCount = new HashMap<String, Integer>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        result.set(sum);
        context.write(key, result);

        String[] parts = key.toString().split("\t");
        if (parts.length == 2) {
            String aspect = parts[0];
            String sentiment = parts[1];
            if (sentiment.equals("positive")) {
                positiveCount.put(aspect, sum);
            } else if (sentiment.equals("negative")) {
                negativeCount.put(aspect, sum);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String maxPosAspect = "";
        int maxPosCount = 0;
        for (Map.Entry<String, Integer> entry : positiveCount.entrySet()) {
            if (entry.getValue() > maxPosCount) {
                maxPosCount = entry.getValue();
                maxPosAspect = entry.getKey();
            }
        }
        
        String maxNegAspect = "";
        int maxNegCount = 0;
        for (Map.Entry<String, Integer> entry : negativeCount.entrySet()) {
            if (entry.getValue() > maxNegCount) {
                maxNegCount = entry.getValue();
                maxNegAspect = entry.getKey();
            }
        }

        context.write(new Text("\nAspect nhận nhiều đánh giá TÍCH CỰC nhất: " + maxPosAspect),
                      new IntWritable(maxPosCount));
        context.write(new Text("Aspect nhận nhiều đánh giá TIÊU CỰC nhất: " + maxNegAspect),
                      new IntWritable(maxNegCount));
    }
}
