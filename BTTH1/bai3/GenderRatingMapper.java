package bai3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String> userGenderMap = new HashMap<String, String>();
    private Text movieId = new Text();
    private Text genderRating = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load users.txt from Distributed Cache: UserID -> Gender
        BufferedReader br = new BufferedReader(new FileReader("users.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            // Format: UserID, Gender, Age, Occupation, Zip-code
            String[] fields = line.split(",\\s*");
            if (fields.length >= 2) {
                String userId = fields[0].trim();
                String gender = fields[1].trim();
                userGenderMap.put(userId, gender);
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
                String userId = fields[0].trim();
                String mid = fields[1].trim();
                String rating = fields[2].trim();

                String gender = userGenderMap.get(userId);
                if (gender != null) {
                    movieId.set(mid);
                    genderRating.set(gender + "|" + rating);
                    context.write(movieId, genderRating);
                }
            } catch (Exception e) {
                // skip invalid lines
            }
        }
    }
}
