/*
 * ===========================================================================
 * PROGETTO BIG DATA 2023/2024 - Università degli Studi di Salerno
 * ===========================================================================
 * 
 * Partecipanti:
 *   1. Cognome: Quagliuolo
 *      Nome: Francesco
 *      Matricola: 0622702412
 *      Email: f.quagliuolo@studenti.unisa.it
 *      Canale: IZ
 * 
 *   2. Cognome: Mangiola
 *      Nome: Giuseppe Alfonso
 *      Matricola: 0622702372
 *      Email: g.mangiola1@studenti.unisa.it
 *      Canale: IZ
 * 
 * ---------------------------------------------------------------------------
 * ESERCIZIO 1 - HADOOP MAPREDUCE
 * ---------------------------------------------------------------------------
 * Titolo: Classifica dei livelli di istruzione per spesa totale
 * 
 * Descrizione:
 *   Questo programma analizza il dataset customer.csv per calcolare la spesa
 *   totale dei clienti raggruppati per livello di istruzione (Education).
 *   L'output finale è una classifica ordinata in modo decrescente per spesa.
 * 
 * Dataset: customer.csv (2240 righe, 29 colonne)
 * 
 * Soluzione:
 *   - Job 1 (Aggregazione): Somma le spese (Wines, Fruits, Meat, Fish, Sweets,
 *     Gold) per ogni livello di istruzione. Utilizza un Combiner per 
 *     ottimizzare il trasferimento dati nella fase di shuffle.
 *   - Job 2 (Ordinamento): Ordina i risultati in modo decrescente usando
 *     il pattern "Value-to-Key Conversion" con chiave negativa.
 * 
 * Pattern utilizzati:
 *   - Summarization Pattern (aggregazione con Combiner)
 *   - Value-to-Key Conversion (per ordinamento decrescente)
 *   - Job Chaining (concatenamento di due job)
 * 
 * ===========================================================================
 */
package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver principale che orchestra l'esecuzione dei due job MapReduce.
 * 
 * Job 1: Aggregazione - calcola la somma delle spese per livello di istruzione
 * Job 2: Ordinamento - ordina i risultati in modo decrescente per spesa totale
 */
public class CustomerDriver {

    /**
     * Metodo main che configura e avvia i job MapReduce in sequenza.
     * 
     * @param args args[0] = path input dataset, args[1] = path output finale
     * @throws Exception in caso di errori durante l'esecuzione dei job
     */
    public static void main(String[] args) throws Exception {
        
        // Validazione argomenti
        if (args.length < 2) {
            System.err.println("Uso: CustomerDriver <input_path> <output_path>");
            System.err.println("  <input_path>  : percorso del file customer.csv su HDFS");
            System.err.println("  <output_path> : percorso della cartella di output su HDFS");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        
        // Definizione dei percorsi
        Path inputPath = new Path(args[0]);
        Path tempPath = new Path("/tmp/customer_intermediate_output");
        Path finalPath = new Path(args[1]);

        // Pulizia cartelle output se esistenti (evita errori di sovrascrittura)
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
            System.out.println("Cartella temporanea eliminata: " + tempPath);
        }
        if (fs.exists(finalPath)) {
            fs.delete(finalPath, true);
            System.out.println("Cartella output eliminata: " + finalPath);
        }

        // =====================================================================
        // JOB 1: AGGREGAZIONE - Somma spese per Education
        // =====================================================================
        System.out.println("\n========== AVVIO JOB 1: Aggregazione ==========");
        
        Job job1 = Job.getInstance(conf, "Job1_Aggregazione_Spesa_Per_Education");
        job1.setJarByClass(CustomerDriver.class);
        
        // Configurazione Mapper, Combiner, Reducer
        job1.setMapperClass(SumMapper.class);
        job1.setCombinerClass(SumReducer.class);  // Ottimizzazione: riduce dati shuffle
        job1.setReducerClass(SumReducer.class);
        
        // Tipi di output
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        // Formati I/O
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        // Percorsi I/O
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempPath);
        
        // Numero di reducer (1 per output ordinato)
        job1.setNumReduceTasks(1);

        // Esecuzione Job 1
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 fallito!");
            System.exit(1);
        }
        System.out.println("========== JOB 1 COMPLETATO ==========\n");

        // =====================================================================
        // JOB 2: ORDINAMENTO - Ranking decrescente per spesa
        // =====================================================================
        System.out.println("========== AVVIO JOB 2: Ordinamento ==========");
        
        Job job2 = Job.getInstance(conf, "Job2_Ranking_Decrescente");
        job2.setJarByClass(CustomerDriver.class);
        
        // Configurazione Mapper e Reducer
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        
        // NOTA: Nel Job 2, il Mapper emette (DoubleWritable, Text)
        // mentre il Reducer emette (Text, DoubleWritable)
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
        // Formati I/O
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        // Percorsi I/O
        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, finalPath);
        
        // Un solo reducer per garantire ordinamento globale
        job2.setNumReduceTasks(1);

        // Esecuzione Job 2
        if (!job2.waitForCompletion(true)) {
            System.err.println("Job 2 fallito!");
            System.exit(2);
        }
        System.out.println("========== JOB 2 COMPLETATO ==========\n");

        // Pulizia cartella temporanea
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
            System.out.println("Cartella temporanea eliminata: " + tempPath);
        }

        System.out.println("============================================");
        System.out.println("ELABORAZIONE COMPLETATA CON SUCCESSO!");
        System.out.println("Output disponibile in: " + finalPath);
        System.out.println("============================================");
    }
}
