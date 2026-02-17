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
 * ESERCIZIO 1 - HADOOP MAPREDUCE - JOB 2 REDUCER (ORDINAMENTO)
 * ---------------------------------------------------------------------------
 * Titolo: Classifica dei livelli di istruzione per spesa totale
 * 
 * Descrizione Reducer:
 *   Riceve i dati già ordinati per spesa (chiave negativa) e ripristina
 *   il formato originale invertendo chiave e valore e tornando al valore
 *   positivo della spesa.
 * 
 *   Input:  (-SpesaTotale, [Education1, Education2, ...])
 *   Output: (Education, SpesaTotale)
 * 
 *   Nota: Più livelli di istruzione potrebbero avere la stessa spesa totale,
 *   quindi il reducer itera su tutti i valori.
 * 
 * ===========================================================================
 */
package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer per il Job 2: Finalizzazione dell'ordinamento.
 * 
 * Riceve i dati ordinati per chiave (spesa negativa) e produce l'output
 * finale nel formato leggibile (Education, SpesaTotale) con valori positivi.
 * 
 * Input:  (-SpesaTotale, Iterable<Education>)
 * Output: (Education, SpesaTotale)
 */
public class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    // Oggetto riutilizzabile per l'output
    private final DoubleWritable outputValue = new DoubleWritable();

    /**
     * Metodo reduce che ripristina il formato originale dell'output.
     * 
     * @param key     spesa totale negativa (per ordinamento decrescente)
     * @param values  livelli di istruzione con quella spesa
     * @param context contesto Hadoop per emettere i risultati
     * @throws IOException in caso di errori I/O
     * @throws InterruptedException se il thread viene interrotto
     */
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        // Ripristina il valore positivo della spesa
        double positiveAmount = -key.get();
        outputValue.set(positiveAmount);

        // Emetti una riga per ogni livello di istruzione con questa spesa
        // (potrebbero esserci più Education con la stessa spesa totale)
        for (Text education : values) {
            context.write(education, outputValue);
        }
    }
}
