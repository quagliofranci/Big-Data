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
 * ESERCIZIO 1 - HADOOP MAPREDUCE - JOB 1 REDUCER/COMBINER
 * ---------------------------------------------------------------------------
 * Titolo: Classifica dei livelli di istruzione per spesa totale
 * 
 * Descrizione Reducer:
 *   Riceve tutte le spese associate a un determinato livello di istruzione
 *   e le somma per ottenere la spesa totale.
 * 
 *   Questa classe viene utilizzata sia come Reducer che come Combiner
 *   (local aggregation) poiché l'operazione di somma è associativa e
 *   commutativa.
 * 
 *   Input:  (Education, [spesa1, spesa2, ...])
 *   Output: (Education, SpesaTotale)
 * 
 * ===========================================================================
 */
package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer per il Job 1: Somma delle spese per livello di istruzione.
 * 
 * Questa classe funge sia da Reducer che da Combiner poiché l'operazione
 * di somma gode delle proprietà di associatività e commutatività, 
 * permettendo aggregazioni locali sui nodi mapper.
 * 
 * Input:  (Education, Iterable<SpesaParziale>)
 * Output: (Education, SpesaTotale)
 */
public class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    // Oggetto riutilizzabile per l'output (ottimizzazione)
    private final DoubleWritable outputValue = new DoubleWritable();

    /**
     * Metodo reduce che somma tutte le spese per un dato livello di istruzione.
     * 
     * @param key     livello di istruzione (es. "Graduation", "PhD", etc.)
     * @param values  iterabile contenente tutte le spese parziali
     * @param context contesto Hadoop per emettere il risultato
     * @throws IOException in caso di errori I/O
     * @throws InterruptedException se il thread viene interrotto
     */
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        // Somma di tutte le spese per questo livello di istruzione
        double totalSum = 0.0;
        
        for (DoubleWritable value : values) {
            totalSum += value.get();
        }

        // Emissione del risultato: (Education, SpesaTotale)
        outputValue.set(totalSum);
        context.write(key, outputValue);
    }
}
