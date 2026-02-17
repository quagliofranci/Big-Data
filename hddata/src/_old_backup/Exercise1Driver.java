import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Exercise1Driver {

    // --- JOB 1: AGGREGAZIONE (Calcola Totale per Education) ---
    public static class SumMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split("\t"); // Attenzione: il dataset originale usa tab o virgola?
            
            // Se il file originale è CSV (virgola), usa questo:
            if (line.contains(",")) cols = line.split(",");

            if (cols.length > 14 && !cols[0].equals("Id")) { // Salta header
                String education = cols[2];
                double totalSpent = 0.0;
                // Somma le colonne di spesa (Vino, Frutta, Carne, Pesce, Dolci, Oro)
                int[] expenseIndices = {9, 10, 11, 12, 13, 14};
                for (int i : expenseIndices) {
                    try { totalSpent += Double.parseDouble(cols[i]); } catch (Exception e) {}
                }
                context.write(new Text(education), new DoubleWritable(totalSpent));
            }
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    // --- JOB 2: ORDINAMENTO (Ranking per Spesa) ---
    // Legge l'output del Job 1 (Education TAB Spesa)
    public static class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");
            if (cols.length == 2) {
                String education = cols[0];
                double amount = Double.parseDouble(cols[1]);
                // Usiamo il valore negativo per ordinare in modo DECRESCENTE (Hadoop ordina crescente di default)
                context.write(new DoubleWritable(-amount), new Text(education));
            }
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Rimettiamo il valore positivo
            double amount = -key.get(); 
            for (Text val : values) {
                context.write(val, new DoubleWritable(amount));
            }
        }
    }

    // --- DRIVER: ORCHESTRA I DUE JOB ---
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Percorsi
        Path inputPath = new Path(args[0]);
        Path tempPath = new Path("/temp_mr_output"); // Cartella temporanea intermedia
        Path finalPath = new Path(args[1]);

        // --- CONFIGURAZIONE JOB 1 ---
        Job job1 = Job.getInstance(conf, "Job 1: Aggregazione Spesa");
        job1.setJarByClass(Exercise1Driver.class);
        job1.setMapperClass(SumMapper.class);
        job1.setCombinerClass(SumReducer.class); // Ottimizzazione
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempPath); // Scrive nella temp

        // Esegui Job 1 e aspetta
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // --- CONFIGURAZIONE JOB 2 ---
        Job job2 = Job.getInstance(conf, "Job 2: Classifica (Ranking)");
        job2.setJarByClass(Exercise1Driver.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        // Nota: Nel mapper 2 la chiave è Double (la spesa) e il valore è Text (Education)
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, tempPath); // Legge dalla temp
        FileOutputFormat.setOutputPath(job2, finalPath); // Scrive nel finale

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
