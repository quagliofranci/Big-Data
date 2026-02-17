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
 * ESERCIZIO 1 - HADOOP MAPREDUCE - JOB 2 MAPPER (ORDINAMENTO)
 * ---------------------------------------------------------------------------
 * Titolo: Classifica dei livelli di istruzione per spesa totale
 * 
 * Descrizione Mapper:
 *   Legge l'output del Job 1 (formato: Education TAB SpesaTotale) e inverte
 *   chiave e valore per permettere l'ordinamento per spesa.
 * 
 *   Per ottenere un ordinamento DECRESCENTE, la spesa viene negata poiché
 *   Hadoop ordina le chiavi in modo crescente per default.
 *   
 *   Pattern utilizzato: Value-to-Key Conversion
 * 
 *   Input:  "Education \t SpesaTotale"
 *   Output: (-SpesaTotale, Education)
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
 * Mapper per il Job 2: Preparazione per ordinamento decrescente.
 * 
 * Utilizza il pattern "Value-to-Key Conversion" per sfruttare l'ordinamento
 * automatico delle chiavi di Hadoop. Negando il valore della spesa,
 * otteniamo un ordinamento decrescente.
 * 
 * Input:  (offset, "Education \t SpesaTotale")
 * Output: (-SpesaTotale, Education)
 */
public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    // Oggetti riutilizzabili per evitare creazione ripetuta
    private final DoubleWritable outputKey = new DoubleWritable();
    private final Text outputValue = new Text();

    /**
     * Metodo map che inverte chiave e valore per l'ordinamento.
     * 
     * @param key     offset in byte della riga (non utilizzato)
     * @param value   riga nel formato "Education \t SpesaTotale"
     * @param context contesto Hadoop per emettere le coppie
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

        // Il formato atteso è: "Education TAB SpesaTotale"
        String[] parts = line.split("\t");
        
        if (parts.length != 2) {
            System.err.println("Formato riga non valido: " + line);
            return;
        }

        try {
            String education = parts[0].trim();
            double amount = Double.parseDouble(parts[1].trim());

            // TRUCCO: Neghiamo la spesa per ottenere ordinamento DECRESCENTE
            // Hadoop ordina le chiavi in modo crescente, quindi:
            // -1000 < -500 < -100 corrisponde a 1000 > 500 > 100
            outputKey.set(-amount);
            outputValue.set(education);
            
            context.write(outputKey, outputValue);

        } catch (NumberFormatException e) {
            System.err.println("Errore parsing valore numerico: " + line);
        }
    }
}
