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

---

## Esercizio 1 - Hadoop MapReduce

### Obiettivo
Individuare i **Top-3 livelli di istruzione** per **spesa totale**.

### Soluzione
Pipeline di 2 job MapReduce concatenati:

**Job 1 - Aggregazione**
- Mapper: estrae (Education, SpesaTotale) per ogni cliente
- Combiner: aggregazione locale per ridurre shuffle
- Reducer: somma le spese per ogni livello di istruzione

**Job 2 - Top-K (K=3)**
- Mapper: mantiene una TreeMap locale con i Top-3, emette in cleanup()
- Reducer: unisce i Top-K parziali in una TreeMap globale, emette in ordine decrescente

### Pattern utilizzati
- **Summarization Pattern** (aggregazione con Combiner)
- **Top-K Pattern** (selezione dei K elementi massimi con TreeMap)
- **Job Chaining** (concatenamento job)

### Struttura codice
```
hddata/src/mapreduce/
├── CustomerDriver.java   # Driver principale
├── SumMapper.java        # Job 1 - Mapper
├── SumReducer.java       # Job 1 - Reducer/Combiner
├── TopKMapper.java       # Job 2 - Mapper (Top-K)
└── TopKReducer.java      # Job 2 - Reducer (Top-K)
```

### Output
```
Graduation      698626.0
PhD             326791.0
Master          226359.0
```

---

## Esercizio 2 - Apache Spark

### Obiettivo
Calcolare il **Web Conversion Rate** (NumWebPurchases / NumWebVisitsMonth) per anno di iscrizione, considerando solo i clienti con Response = 1.

### Soluzione
Pipeline RDD: filter → mapToPair → reduceByKey → mapValues → sortByKey → saveAsTextFile

### Struttura codice
```
hddata/src/spark/
└── SparkDriver.java
```

### Output
```
2012    0.8518    (85.18%)
2013    1.0664    (106.64%)
2014    1.0       (100%)
```

---



---

## Esecuzione

### Avvio cluster
```bash
cd /home/quaily/progetto_bigdata
docker-compose up -d
docker exec -it master bash

export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
export PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin

# Solo la prima volta
hdfs namenode -format
start-dfs.sh
start-yarn.sh
```

### Esecuzione MapReduce
```bash
cd /data
./scripts/run_mapreduce.sh
```

### Esecuzione Spark
```bash
cd /data
./scripts/run_spark_local.sh
```

### Spegnimento
```bash
stop-yarn.sh
stop-dfs.sh
exit
docker-compose down
```
