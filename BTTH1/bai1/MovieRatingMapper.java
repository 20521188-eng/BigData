package bai1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text movieId = new Text();
    private DoubleWritable rating = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Format: UserID, MovieID, Rating, Timestamp
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] fields = line.split(",\\s*");
        if (fields.length >= 3) {
            try {
                String mid = fields[1].trim();
                double r = Double.parseDouble(fields[2].trim());
                movieId.set(mid);
                rating.set(r);
                context.write(movieId, rating);
            } catch (NumberFormatException e) {
                // skip invalid lines
            }
        }
    }
}
