package bai4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, String> movieMap = new HashMap<String, String>();

    // Age groups in order
    private static final String[] AGE_GROUPS = {"0-18", "18-35", "35-50", "50+"};

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load movies.txt: MovieID -> Title
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
        // Accumulators for each age group
        Map<String, Double> sumMap = new HashMap<String, Double>();
        Map<String, Integer> countMap = new HashMap<String, Integer>();

        for (String ag : AGE_GROUPS) {
            sumMap.put(ag, 0.0);
            countMap.put(ag, 0);
        }

        for (Text val : values) {
            // Format: AgeGroup|Rating
            String[] parts = val.toString().split("\\|");
            if (parts.length >= 2) {
                String ageGroup = parts[0].trim();
                double rating = Double.parseDouble(parts[1].trim());

                if (sumMap.containsKey(ageGroup)) {
                    sumMap.put(ageGroup, sumMap.get(ageGroup) + rating);
                    countMap.put(ageGroup, countMap.get(ageGroup) + 1);
                }
            }
        }

        String movieTitle = movieMap.get(key.toString());
        if (movieTitle == null) {
            movieTitle = "Unknown(" + key.toString() + ")";
        }

        // Build output: 0-18: avg  18-35: avg  35-50: avg  50+: avg
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < AGE_GROUPS.length; i++) {
            String ag = AGE_GROUPS[i];
            int count = countMap.get(ag);
            String avgStr;
            if (count > 0) {
                double avg = sumMap.get(ag) / count;
                avgStr = String.format("%.2f", avg);
            } else {
                avgStr = "NA";
            }

            sb.append(ag).append(": ").append(avgStr);
            if (i < AGE_GROUPS.length - 1) {
                sb.append("  ");
            }
        }

        context.write(new Text(movieTitle), new Text(sb.toString()));
    }
}
