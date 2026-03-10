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
 * ESERCIZIO 1 - HADOOP MAPREDUCE - JOB 2 REDUCER (TOP-K)
 * ---------------------------------------------------------------------------
 * Titolo: Classifica dei livelli di istruzione per spesa totale
 * 
 * Descrizione Reducer:
 *   Riceve i Top-K parziali da ogni Mapper e li unisce in un'unica
 *   TreeMap globale, mantenendo solo i K record con spesa più alta.
 *   Alla fine, emette i risultati in ordine decrescente di spesa.
 *   
 *   Pattern utilizzato: Top-K
 * 
 *   Input:  (SpesaTotale, [Education1, Education2, ...])
 *   Output: (Education, SpesaTotale) — Top-K globale, ordinato decrescente
 * 
 * ===========================================================================
 */
package mapreduce;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer per il Job 2: Unione dei Top-K parziali e output finale.
 * 
 * Riceve i Top-K locali da tutti i mapper e li combina in una TreeMap
 * globale. Poiché più mapper possono produrre gli stessi record o record
 * diversi, il reducer garantisce che il risultato finale contenga
 * esattamente i K record con spesa più alta.
 * 
 * L'output viene emesso in ordine DECRESCENTE nella fase di cleanup(),
 * iterando la TreeMap dal valore più alto al più basso.
 * 
 * Input:  (SpesaTotale, Iterable<Education>)
 * Output: (Education, SpesaTotale) — Top-K globale ordinato decrescente
 */
public class TopKReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    // Valore di K (top 3)
    private static final int K = 3;

    // TreeMap globale per unire i Top-K di tutti i mapper
    private TreeMap<Double, String> topK = new TreeMap<>();

    /**
     * Metodo reduce: inserisce i record nella TreeMap globale.
     * Mantiene solo i K record con spesa più alta.
     * 
     * @param key     spesa totale
     * @param values  livelli di istruzione con quella spesa
     * @param context contesto Hadoop (output emesso in cleanup)
     */
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double amount = key.get();

        for (Text education : values) {
            // Inserisci nella TreeMap globale
            topK.put(amount, education.toString());

            // Mantieni solo K elementi
            if (topK.size() > K) {
                topK.remove(topK.firstKey());
            }
        }
    }

    /**
     * Metodo cleanup: emette i Top-K globali in ordine DECRESCENTE.
     * Utilizza descendingMap() per iterare dalla spesa più alta alla più bassa.
     * 
     * @param context contesto Hadoop per emettere i risultati finali
     */
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        // Itera in ordine decrescente (dalla spesa più alta alla più bassa)
        NavigableMap<Double, String> descending = topK.descendingMap();

        for (Map.Entry<Double, String> entry : descending.entrySet()) {
            context.write(
                new Text(entry.getValue()),
                new DoubleWritable(entry.getKey())
            );
        }
    }
}
