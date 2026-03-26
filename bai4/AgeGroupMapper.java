package bai4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AgeGroupMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, Integer> userAgeMap = new HashMap<String, Integer>();
    private Text movieId = new Text();
    private Text ageGroupRating = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load users.txt: UserID -> Age
        BufferedReader br = new BufferedReader(new FileReader("users.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            // Format: UserID, Gender, Age, Occupation, Zip-code
            String[] fields = line.split(",\\s*");
            if (fields.length >= 3) {
                try {
                    String userId = fields[0].trim();
                    int age = Integer.parseInt(fields[2].trim());
                    userAgeMap.put(userId, age);
                } catch (NumberFormatException e) {
                    // skip
                }
            }
        }
        br.close();
    }

    private String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        else if (age <= 35) return "18-35";
        else if (age <= 50) return "35-50";
        else return "50+";
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

                Integer age = userAgeMap.get(userId);
                if (age != null) {
                    String ageGroup = getAgeGroup(age);
                    movieId.set(mid);
                    ageGroupRating.set(ageGroup + "|" + rating);
                    context.write(movieId, ageGroupRating);
                }
            } catch (Exception e) {
                // skip
            }
        }
    }
}
