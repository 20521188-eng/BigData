package bai2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenreRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Map<String, String> movieGenreMap = new HashMap<String, String>();
    private Text genre = new Text();
    private DoubleWritable rating = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load movies.txt from Distributed Cache
        BufferedReader br = new BufferedReader(new FileReader("movies.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            // Format: MovieID, Title, Genres
            String[] fields = line.split(",\\s*", 3);
            if (fields.length >= 3) {
                String movieId = fields[0].trim();
                String genres = fields[2].trim();
                movieGenreMap.put(movieId, genres);
            }
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Format: UserID, MovieID, Rating, Timestamp
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] fields = line.split(",\\s*");
        if (fields.length >= 3) {
            try {
                String movieId = fields[1].trim();
                double r = Double.parseDouble(fields[2].trim());

                String genres = movieGenreMap.get(movieId);
                if (genres != null) {
                    // Split genres by "|" and emit for each genre
                    String[] genreList = genres.split("\\|");
                    for (String g : genreList) {
                        genre.set(g.trim());
                        rating.set(r);
                        context.write(genre, rating);
                    }
                }
            } catch (NumberFormatException e) {
                // skip invalid lines
            }
        }
    }
}
