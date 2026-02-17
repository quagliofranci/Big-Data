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
 * ESERCIZIO 1 - HADOOP MAPREDUCE - JOB 1 MAPPER
 * ---------------------------------------------------------------------------
 * Titolo: Classifica dei livelli di istruzione per spesa totale
 * 
 * Descrizione Mapper:
 *   Legge ogni riga del dataset customer.csv, estrae il livello di istruzione
 *   (colonna Education) e calcola la spesa totale sommando le colonne:
 *   MntWines, MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, 
 *   MntGoldProds (indici 9-14).
 * 
 *   Output: (Education, SpesaTotale)
 * 
 * ===========================================================================
 */
package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper per il Job 1: Aggregazione delle spese per livello di istruzione.
 * 
 * Input:  (offset, riga CSV)
 * Output: (Education, SpesaTotale)
 * 
 * Il Mapper processa ogni riga del file CSV, salta l'header, e per ogni
 * cliente emette una coppia (livello_istruzione, spesa_totale).
 */
public class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    // Oggetti riutilizzabili per evitare creazione ripetuta (ottimizzazione)
    private final Text outputKey = new Text();
    private final DoubleWritable outputValue = new DoubleWritable();

    // Indici delle colonne di spesa nel CSV (0-indexed)
    // 9=MntWines, 10=MntFruits, 11=MntMeatProducts, 
    // 12=MntFishProducts, 13=MntSweetProducts, 14=MntGoldProds
    private static final int[] EXPENSE_COLUMNS = {9, 10, 11, 12, 13, 14};
    
    // Indice della colonna Education
    private static final int EDUCATION_INDEX = 2;
    
    // Numero minimo di colonne attese
    private static final int MIN_COLUMNS = 15;

    /**
     * Metodo map che processa ogni riga del dataset.
     * 
     * @param key    offset in byte della riga nel file (non utilizzato)
     * @param value  contenuto della riga CSV
     * @param context contesto Hadoop per emettere le coppie chiave-valore
     * @throws IOException in caso di errori I/O
     * @throws InterruptedException se il thread viene interrotto
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        
        // Salta righe vuote
        if (line.isEmpty()) {
            return;
        }
        
        // Salta la riga di header (case-insensitive)
        if (line.toLowerCase().startsWith("id,") || line.contains("Year_Birth")) {
            return;
        }

        // Split della riga CSV
        String[] columns = line.split(",", -1);  // -1 per mantenere campi vuoti
        
        // Verifica numero minimo di colonne
        if (columns.length < MIN_COLUMNS) {
            // Riga malformata, skip
            return;
        }

        try {
            // Estrazione livello di istruzione
            String education = columns[EDUCATION_INDEX].trim();
            
            // Validazione: education non deve essere vuoto
            if (education.isEmpty()) {
                return;
            }

            // Calcolo spesa totale sommando le colonne di spesa
            double totalSpent = 0.0;
            for (int colIndex : EXPENSE_COLUMNS) {
                if (colIndex < columns.length) {
                    String val = columns[colIndex].trim();
                    if (!val.isEmpty()) {
                        totalSpent += parseDouble(val);
                    }
                }
            }

            // Emissione coppia (Education, SpesaTotale)
            outputKey.set(education);
            outputValue.set(totalSpent);
            context.write(outputKey, outputValue);

        } catch (Exception e) {
            // In caso di errore di parsing, logga e continua
            System.err.println("Errore parsing riga: " + line + " - " + e.getMessage());
        }
    }

    /**
     * Metodo helper per parsing sicuro di un valore double.
     * 
     * @param value stringa da convertire
     * @return valore double, 0.0 se parsing fallisce
     */
    private double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
