package bai1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Map<String, String> movieMap = new HashMap<String, String>();
    private String maxMovie = "";
    private double maxRating = 0.0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load movies.txt from Distributed Cache
        // File will be symlinked to current working directory
        BufferedReader br = new BufferedReader(new FileReader("movies.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            // Format: MovieID, Title, Genres
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
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        double avg = sum / count;
        // Round to 1 decimal (matching output format)
        avg = Math.round(avg * 100.0) / 100.0;

        String movieTitle = movieMap.get(key.toString());
        if (movieTitle == null) {
            movieTitle = "Unknown(" + key.toString() + ")";
        }

        String result = "Average rating: " + String.format("%.1f", avg)
                       + " (Total ratings: " + count + ")";
        context.write(new Text(movieTitle), new Text(result));

        // Track highest rated movie (with at least 5 ratings)
        if (count >= 5 && avg > maxRating) {
            maxRating = avg;
            maxMovie = movieTitle;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the highest rated movie at the end
        if (!maxMovie.isEmpty()) {
            String msg = maxMovie + " is the highest rated movie with an average rating of "
                       + String.format("%.1f", maxRating)
                       + " among movies with at least 5 ratings.";
            context.write(new Text(msg), new Text(""));
        }
    }
}
