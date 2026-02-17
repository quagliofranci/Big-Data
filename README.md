# Progetto Big Data 2023/2024 - Customer Analysis

## Informazioni Progetto

**Università degli Studi di Salerno**  
**Corso di Big Data 2023/2024**

### Partecipanti

| Cognome | Nome | Matricola | Email | Canale |
|---------|------|-----------|-------|--------|
| Quagliuolo | Francesco | 0622702412 | f.quagliuolo@studenti.unisa.it | IZ |
| Mangiola | Giuseppe Alfonso | 0622702372 | g.mangiola1@studenti.unisa.it | IZ |

---

## Dataset

Il progetto utilizza il dataset **customer.csv** contenente informazioni sui clienti di un'azienda.

- **Righe**: 2240
- **Colonne**: 29

### Descrizione colonne principali

| Colonna | Descrizione |
|---------|-------------|
| ID | Identificativo cliente |
| Year_Birth | Anno di nascita |
| Education | Livello di istruzione |
| Marital_Status | Stato civile |
| Income | Reddito annuo |
| Dt_Customer | Data di iscrizione |
| MntWines, MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, MntGoldProds | Spese per categoria |
| NumWebPurchases | Numero acquisti via web |
| NumWebVisitsMonth | Visite web mensili |
| Response | Risposta all'ultima campagna (1=Sì, 0=No) |

---

## Esercizio 1 - Hadoop MapReduce

### Obiettivo
Creare una **classifica dei livelli di istruzione** ordinata per **spesa totale decrescente**.

### Soluzione
Pipeline di 2 job MapReduce concatenati:

**Job 1 - Aggregazione**
- Mapper: estrae (Education, SpesaTotale) per ogni cliente
- Combiner: aggregazione locale per ridurre shuffle
- Reducer: somma le spese per ogni livello di istruzione

**Job 2 - Ordinamento**
- Mapper: inverte chiave/valore con spesa negativa (Value-to-Key Conversion)
- Reducer: ripristina formato originale con ordinamento decrescente

### Pattern utilizzati
- **Summarization Pattern** (aggregazione con Combiner)
- **Value-to-Key Conversion** (ordinamento decrescente)
- **Job Chaining** (concatenamento job)

### Struttura codice
```
hddata/src/mapreduce/
├── CustomerDriver.java   # Driver principale
├── SumMapper.java        # Job 1 - Mapper
├── SumReducer.java       # Job 1 - Reducer/Combiner
├── SortMapper.java       # Job 2 - Mapper
└── SortReducer.java      # Job 2 - Reducer
```

### Compilazione ed Esecuzione

```bash
# Accedi al container master
docker exec -it master bash

# Vai alla cartella dati
cd /data

# Compila
mkdir -p build
javac -cp "$(hadoop classpath)" -d build $(find src/mapreduce -name "*.java")

# Crea JAR
jar -cvf CustomerAnalysis.jar -C build .

# Carica dataset su HDFS
hdfs dfs -mkdir -p /input
hdfs dfs -put dataset/customer.csv /input/

# Esegui
hadoop jar CustomerAnalysis.jar mapreduce.CustomerDriver hdfs:///input/customer.csv hdfs:///output_mapreduce

# Visualizza risultati
hdfs dfs -cat hdfs:///output_mapreduce/part-r-00000

# Scarica output in locale
hdfs dfs -get hdfs:///output_mapreduce /data/
```

In alternativa, usa lo script:
```bash
chmod +x /data/scripts/run_mapreduce.sh
/data/scripts/run_mapreduce.sh
```

### Output atteso
```
PhD             1388559.0
Graduation      1217553.0
Master          620998.0
2n Cycle        503185.0
Basic           50831.0
```

---

## Esercizio 2 - Apache Spark

### Obiettivo
Calcolare il **Web Conversion Rate** (tasso di conversione web) per anno di iscrizione, considerando solo i clienti che hanno risposto positivamente all'ultima campagna marketing (Response = 1).

### Formula
```
Web Conversion Rate = NumWebPurchases / NumWebVisitsMonth
```

### Soluzione
1. Filtra clienti con Response = 1
2. Raggruppa per anno di iscrizione (estratto da Dt_Customer)
3. Calcola somma di WebPurchases e WebVisits per anno
4. Calcola il tasso: Sum(Purchases) / Sum(Visits)
5. Ordina per anno

### Struttura codice
```
hddata/src/spark/
└── SparkDriver.java   # Driver Spark
```

### Compilazione ed Esecuzione

**Modalità Locale:**
```bash
# Accedi al container master
docker exec -it master bash
cd /data

# Compila
mkdir -p build_spark
javac -cp "$SPARK_HOME/jars/*" -d build_spark $(find src/spark -name "*.java")

# Crea JAR
jar -cvf CustomerSpark.jar -C build_spark .

# Esegui in locale
spark-submit --class spark.SparkDriver --master local[*] CustomerSpark.jar dataset/customer.csv output_spark

# Visualizza risultati
cat output_spark/part-*
```

**Modalità Cluster (HDFS):**
```bash
# Carica su HDFS (se non già fatto)
hdfs dfs -put dataset/customer.csv /input/

# Esegui con I/O su HDFS
spark-submit --class spark.SparkDriver --master local[*] CustomerSpark.jar hdfs:///input/customer.csv hdfs:///output_spark

# Visualizza risultati
hdfs dfs -cat hdfs:///output_spark/part-*

# Scarica in locale
hdfs dfs -get hdfs:///output_spark /data/
```

In alternativa, usa gli script:
```bash
chmod +x /data/scripts/run_spark_local.sh
chmod +x /data/scripts/run_spark_cluster.sh

# Esecuzione locale
/data/scripts/run_spark_local.sh

# Esecuzione cluster
/data/scripts/run_spark_cluster.sh
```

### Output atteso
```
2012	0.8518	(85.18%)
2013	1.0664	(106.64%)
2014	1.0     (100%)
```

---

## Struttura del Progetto

```
progetto_bigdata/
├── README.md                      # Questo file
├── docker-compose.yml             # Configurazione Docker
├── hadoop.env                     # Variabili ambiente Hadoop
├── documentation/                 # Report e documentazione
│   └── (Report PDF da aggiungere)
├── scripts/                       # Script di esecuzione
│   ├── run_mapreduce.sh
│   ├── run_spark_local.sh
│   └── run_spark_cluster.sh
└── hddata/
    ├── dataset/
    │   └── customer.csv           # Dataset
    ├── src/
    │   ├── mapreduce/             # Codice MapReduce
    │   │   ├── CustomerDriver.java
    │   │   ├── SumMapper.java
    │   │   ├── SumReducer.java
    │   │   ├── SortMapper.java
    │   │   └── SortReducer.java
    │   └── spark/                 # Codice Spark
    │       └── SparkDriver.java
    ├── CustomerAnalysis.jar       # JAR MapReduce (generato)
    ├── CustomerSpark.jar          # JAR Spark (generato)
    ├── output_mapreduce/          # Output MapReduce
    └── output_spark/              # Output Spark
```

---

## Docker

### Avvio cluster
```bash
cd progetto_bigdata
docker-compose up -d
```

### Accesso al container master
```bash
docker exec -it master bash
```

### Stop cluster
```bash
docker-compose down
```

---

## Note

- Java 8 utilizzato per compatibilità con Hadoop e Spark
- I JAR contengono solo il codice utente, non le librerie di framework
- Il Combiner nel Job 1 di MapReduce ottimizza il trasferimento dati
- Spark utilizza le RDD API con lambda expressions

---

## Pulizia HDFS

```bash
# Rimuovi output MapReduce
hdfs dfs -rm -r hdfs:///output_mapreduce

# Rimuovi output Spark
hdfs dfs -rm -r hdfs:///output_spark

# Rimuovi input
hdfs dfs -rm -r hdfs:///input
```
