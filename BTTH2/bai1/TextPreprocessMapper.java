package bai1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TextPreprocessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Set<String> stopwords = new HashSet<String>();
    private Text wordOut = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";", -1);
        if (fields.length < 5) return;

        String comment = fields[1].trim().toLowerCase();
        comment = comment.replaceAll("[^\\p{L}\\s]", " ");

        String[] words = comment.split("\\s+");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty() && !stopwords.contains(word)) {
                wordOut.set(word);
                context.write(wordOut, one);
            }
        }
    }
}
