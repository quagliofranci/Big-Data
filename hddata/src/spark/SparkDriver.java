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
 * ESERCIZIO 2 - APACHE SPARK
 * ---------------------------------------------------------------------------
 * Titolo: Analisi del tasso di conversione web per anno di iscrizione
 * 
 * Descrizione:
 *   Questo programma Spark analizza il dataset customer.csv per calcolare
 *   il tasso di conversione web (Web Conversion Rate) dei clienti che hanno
 *   risposto positivamente all'ultima campagna marketing (Response = 1),
 *   raggruppati per anno di iscrizione.
 * 
 *   Il Web Conversion Rate è definito come:
 *     WCR = NumWebPurchases / NumWebVisitsMonth
 *   
 *   Rappresenta l'efficacia nel convertire le visite web in acquisti.
 * 
 * Dataset: customer.csv (2240 righe, 29 colonne)
 * 
 * Colonne utilizzate:
 *   - Dt_Customer (indice 7): data di iscrizione del cliente (formato dd-MM-yyyy)
 *   - NumWebPurchases (indice 16): numero di acquisti via web
 *   - NumWebVisitsMonth (indice 19): numero di visite web mensili
 *   - Response (indice 28): risposta all'ultima campagna (1 = sì, 0 = no)
 * 
 * Soluzione:
 *   1. Filtra i clienti che hanno risposto positivamente (Response = 1)
 *   2. Raggruppa per anno di iscrizione
 *   3. Calcola la somma di WebPurchases e WebVisits per anno
 *   4. Calcola il tasso di conversione aggregato: Sum(Purchases) / Sum(Visits)
 *   5. Ordina per anno e salva i risultati
 * 
 * API utilizzate: RDD con operazioni mapToPair, filter, reduceByKey, sortByKey
 * 
 * ===========================================================================
 */
package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Driver Spark per l'analisi del tasso di conversione web.
 * 
 * Calcola il Web Conversion Rate (acquisti web / visite web) per i clienti
 * che hanno risposto positivamente alla campagna marketing, raggruppati
 * per anno di iscrizione.
 */
public class SparkDriver {

    // Indici delle colonne nel CSV (0-indexed)
    private static final int DT_CUSTOMER_INDEX = 7;      // Data iscrizione
    private static final int WEB_PURCHASES_INDEX = 16;   // NumWebPurchases
    private static final int WEB_VISITS_INDEX = 19;      // NumWebVisitsMonth
    private static final int RESPONSE_INDEX = 28;        // Response (0 o 1)
    
    // Numero minimo di colonne attese
    private static final int MIN_COLUMNS = 29;

    /**
     * Metodo main che esegue l'analisi Spark.
     * 
     * @param args args[0] = path input, args[1] = path output
     */
    public static void main(String[] args) {
        
        // Validazione argomenti
        if (args.length < 2) {
            System.err.println("Uso: SparkDriver <input_path> <output_path>");
            System.err.println("  <input_path>  : percorso del file customer.csv");
            System.err.println("  <output_path> : percorso della cartella di output");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Configurazione Spark
        SparkConf conf = new SparkConf()
                .setAppName("WebConversionRateAnalysis");
        
        // Creazione del contesto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            System.out.println("\n========================================");
            System.out.println("AVVIO ANALISI WEB CONVERSION RATE");
            System.out.println("========================================\n");

            // =========================================================
            // STEP 1: Lettura del file CSV
            // =========================================================
            JavaRDD<String> lines = sc.textFile(inputPath);
            
            // Identifica e rimuovi l'header
            String header = lines.first();
            JavaRDD<String> data = lines.filter(line -> !line.equals(header));

            // =========================================================
            // STEP 2: Filtra clienti con Response=1 e mappa a (Anno, (Purchases, Visits))
            // =========================================================
            JavaPairRDD<Integer, Tuple2<Double, Double>> yearlyData = data
                .mapToPair(line -> {
                    String[] cols = line.split(",", -1);
                    
                    // Validazione numero colonne
                    if (cols.length < MIN_COLUMNS) {
                        return null;
                    }
                    
                    // Filtra solo clienti con Response = 1
                    String responseStr = cols[RESPONSE_INDEX].trim();
                    if (!responseStr.equals("1")) {
                        return null;
                    }

                    try {
                        // Parsing della data di iscrizione
                        String dateStr = cols[DT_CUSTOMER_INDEX].trim();
                        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
                        Date date = sdf.parse(dateStr);
                        
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(date);
                        int year = calendar.get(Calendar.YEAR);

                        // Parsing dei valori numerici
                        double webPurchases = parseDouble(cols[WEB_PURCHASES_INDEX].trim());
                        double webVisits = parseDouble(cols[WEB_VISITS_INDEX].trim());

                        // Emetti (Anno, (WebPurchases, WebVisits))
                        return new Tuple2<>(year, new Tuple2<>(webPurchases, webVisits));
                        
                    } catch (Exception e) {
                        // In caso di errore di parsing, ritorna null
                        return null;
                    }
                })
                .filter(tuple -> tuple != null);  // Rimuovi i null

            // =========================================================
            // STEP 3: Aggregazione per anno (somma Purchases e Visits)
            // =========================================================
            JavaPairRDD<Integer, Tuple2<Double, Double>> aggregated = yearlyData
                .reduceByKey((v1, v2) -> new Tuple2<>(
                    v1._1() + v2._1(),   // Somma WebPurchases
                    v1._2() + v2._2()    // Somma WebVisits
                ));

            // =========================================================
            // STEP 4: Calcolo del Conversion Rate e ordinamento
            // =========================================================
            JavaPairRDD<Integer, Double> conversionRates = aggregated
                .mapValues(v -> {
                    double purchases = v._1();
                    double visits = v._2();
                    // Evita divisione per zero
                    if (visits == 0) {
                        return 0.0;
                    }
                    return purchases / visits;
                })
                .sortByKey();  // Ordina per anno crescente

            // =========================================================
            // STEP 5: Formattazione output e salvataggio
            // =========================================================
            DecimalFormat df = new DecimalFormat("#.####");
            
            JavaRDD<String> formattedOutput = conversionRates
                .map(tuple -> {
                    int year = tuple._1();
                    double rate = tuple._2();
                    // Formato: "Anno TAB ConversionRate TAB Percentuale"
                    return year + "\t" + df.format(rate) + "\t(" + df.format(rate * 100) + "%)";
                });

            // Salvataggio su file
            formattedOutput.saveAsTextFile(outputPath);

            // =========================================================
            // STEP 6: Stampa risultati a console
            // =========================================================
            System.out.println("========================================");
            System.out.println("RISULTATI - Web Conversion Rate per Anno");
            System.out.println("(Solo clienti con Response = 1)");
            System.out.println("========================================");
            System.out.println("Anno\tConversion Rate\tPercentuale");
            System.out.println("----------------------------------------");
            
            for (Tuple2<Integer, Double> result : conversionRates.collect()) {
                System.out.println(result._1() + "\t" + df.format(result._2()) + 
                                   "\t\t(" + df.format(result._2() * 100) + "%)");
            }
            
            System.out.println("========================================\n");
            System.out.println("Output salvato in: " + outputPath);
            System.out.println("Elaborazione completata con successo!");

        } finally {
            // Chiusura del contesto Spark
            sc.close();
        }
    }

    /**
     * Metodo helper per parsing sicuro di valori double.
     * 
     * @param value stringa da convertire
     * @return valore double, 0.0 se il parsing fallisce
     */
    private static double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
