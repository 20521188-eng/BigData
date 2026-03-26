package bai3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenderRatingReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, String> movieMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load movies.txt from Distributed Cache: MovieID -> Title
        BufferedReader br = new BufferedReader(new FileReader("movies.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",\\s*", 3);
            if (fields.length >= 2) {
                String movieId = fields[0].trim();
                String title = fields[1].trim();
                movieMap.put(movieId, title);
            }
        }
        br.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double maleSum = 0.0, femaleSum = 0.0;
        int maleCount = 0, femaleCount = 0;

        for (Text val : values) {
            // Format: Gender|Rating
            String[] parts = val.toString().split("\\|");
            if (parts.length >= 2) {
                String gender = parts[0].trim();
                double rating = Double.parseDouble(parts[1].trim());

                if ("M".equals(gender)) {
                    maleSum += rating;
                    maleCount++;
                } else if ("F".equals(gender)) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }
        }

        String movieTitle = movieMap.get(key.toString());
        if (movieTitle == null) {
            movieTitle = "Unknown(" + key.toString() + ")";
        }

        String maleAvg = maleCount > 0 ? String.format("%.2f", maleSum / maleCount) : "NA";
        String femaleAvg = femaleCount > 0 ? String.format("%.2f", femaleSum / femaleCount) : "NA";

        String result = "Male: " + maleAvg + ", Female: " + femaleAvg;
        context.write(new Text(movieTitle), new Text(result));
    }
}
