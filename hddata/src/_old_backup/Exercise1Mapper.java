import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Exercise1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Salta header
        if (line.startsWith("ID") || line.contains("Year_Birth")) return;

        String[] cols = line.split(","); 
        try {
            String education = cols[2];
            // Somma spese (indici 9-14)
            double totalSpent = 0;
            int[] expenseIndices = {9, 10, 11, 12, 13, 14};
            for (int i : expenseIndices) {
                if(i < cols.length && !cols[i].isEmpty()) totalSpent += Double.parseDouble(cols[i]);
            }
            context.write(new Text(education), new DoubleWritable(totalSpent));
        } catch (Exception e) {}
    }
}
