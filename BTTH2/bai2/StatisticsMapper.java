package bai2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatisticsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Set<String> stopwords = new HashSet<String>();
    private Text keyOut = new Text();
    private final static IntWritable one = new IntWritable(1);
    private String jobType;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        jobType = context.getConfiguration().get("job.type", "wordfreq");

        if (jobType.equals("wordfreq")) {
            BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim().toLowerCase();
                if (!line.isEmpty()) {
                    stopwords.add(line);
                }
            }
            br.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";", -1);
        if (fields.length < 5) return;

        String comment = fields[1].trim();
        String category = fields[2].trim();
        String aspect = fields[3].trim();

        if (jobType.equals("wordfreq")) {
            String processed = comment.toLowerCase().replaceAll("[^\\p{L}\\s]", " ");
            String[] words = processed.split("\\s+");
            for (String word : words) {
                word = word.trim();
                if (!word.isEmpty() && !stopwords.contains(word)) {
                    keyOut.set(word);
                    context.write(keyOut, one);
                }
            }
        } else if (jobType.equals("category")) {
            keyOut.set(category);
            context.write(keyOut, one);
        } else if (jobType.equals("aspect")) {
            keyOut.set(aspect);
            context.write(keyOut, one);
        }
    }
}
