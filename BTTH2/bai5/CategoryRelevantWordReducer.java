package bai5;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoryRelevantWordReducer extends Reducer<Text, IntWritable, Text, Text> {

    private Map<String, TreeMap<Integer, List<String>>> topWords =
            new HashMap<String, TreeMap<Integer, List<String>>>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        String[] parts = key.toString().split("::", 2);
        if (parts.length < 2) return;

        String category = parts[0];
        String word = parts[1];

        if (!topWords.containsKey(category)) {
            topWords.put(category, new TreeMap<Integer, List<String>>(Collections.reverseOrder()));
        }

        TreeMap<Integer, List<String>> map = topWords.get(category);
        if (!map.containsKey(sum)) {
            map.put(sum, new ArrayList<String>());
        }
        map.get(sum).add(word);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, TreeMap<Integer, List<String>>> entry : topWords.entrySet()) {
            String category = entry.getKey();
            TreeMap<Integer, List<String>> map = entry.getValue();

            List<String> top5 = new ArrayList<String>();
            for (Map.Entry<Integer, List<String>> wordEntry : map.entrySet()) {
                for (String word : wordEntry.getValue()) {
                    top5.add(word + "(" + wordEntry.getKey() + ")");
                    if (top5.size() >= 5) break;
                }
                if (top5.size() >= 5) break;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < top5.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(top5.get(i));
            }

            context.write(new Text("[" + category + "]"), new Text(sb.toString()));
        }
    }
}
