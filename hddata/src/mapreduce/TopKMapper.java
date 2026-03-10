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
 * ESERCIZIO 1 - HADOOP MAPREDUCE - JOB 2 MAPPER (TOP-K)
 * ---------------------------------------------------------------------------
 * Titolo: Classifica dei livelli di istruzione per spesa totale
 * 
 * Descrizione Mapper:
 *   Legge l'output del Job 1 (formato: Education TAB SpesaTotale) e mantiene
 *   in memoria solo i Top-K elementi con spesa maggiore, utilizzando una
 *   TreeMap ordinata per spesa.
 *   
 *   La TreeMap mantiene al massimo K elementi: quando la dimensione supera K,
 *   viene rimosso l'elemento con la chiave più bassa (spesa minore).
 *   I Top-K locali vengono emessi nella fase di cleanup().
 *   
 *   Pattern utilizzato: Top-K
 * 
 *   Input:  "Education \t SpesaTotale"
 *   Output: (SpesaTotale, Education) — solo i Top-K locali
 * 
 * ===========================================================================
 */
package mapreduce;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper per il Job 2: Selezione dei Top-K livelli di istruzione per spesa.
 * 
 * Utilizza una TreeMap locale per mantenere solo i K record con spesa
 * più alta. La TreeMap è ordinata per chiave (spesa) in modo crescente,
 * quindi il primo elemento è sempre quello con la spesa più bassa:
 * quando la mappa supera K elementi, si rimuove il primo (il minore).
 * 
 * I Top-K locali vengono emessi in cleanup() per ridurre il traffico
 * nella fase di shuffle.
 * 
 * Input:  (offset, "Education \t SpesaTotale")
 * Output: (SpesaTotale, Education)
 */
public class TopKMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    // Valore di K (top 3)
    private static final int K = 3;

    // TreeMap locale ordinata per spesa (crescente).
    // Chiave: spesa (Double), Valore: education (String)
    // La TreeMap mantiene l'ordinamento naturale delle chiavi,
    // quindi il primo elemento è quello con spesa minore.
    private TreeMap<Double, String> topK = new TreeMap<>();

    /**
     * Metodo map: inserisce ogni record nella TreeMap locale.
     * Se la dimensione supera K, rimuove il record con spesa minore.
     * 
     * @param key     offset in byte della riga (non utilizzato)
     * @param value   riga nel formato "Education \t SpesaTotale"
     * @param context contesto Hadoop (non utilizzato in map, solo in cleanup)
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

            // Inserisci nella TreeMap
            topK.put(amount, education);

            // Se la mappa supera K elementi, rimuovi quello con spesa minore
            if (topK.size() > K) {
                topK.remove(topK.firstKey());
            }

        } catch (NumberFormatException e) {
            System.err.println("Errore parsing valore numerico: " + line);
        }
    }

    /**
     * Metodo cleanup: emette i Top-K locali al termine del processing.
     * Viene chiamato una sola volta dopo che tutte le righe sono state
     * processate dal map(). I record vengono emessi con la spesa come
     * chiave per permettere l'ordinamento nel reducer.
     * 
     * @param context contesto Hadoop per emettere le coppie chiave-valore
     */
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        // Emetti tutti i Top-K locali
        for (Map.Entry<Double, String> entry : topK.entrySet()) {
            context.write(
                new DoubleWritable(entry.getKey()),
                new Text(entry.getValue())
            );
        }
    }
}
