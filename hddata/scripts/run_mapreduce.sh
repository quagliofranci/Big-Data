#!/bin/bash
# ============================================================================
# PROGETTO BIG DATA 2023/2024 - Script Esecuzione MapReduce
# ============================================================================
# 
# Studenti:
#   - Francesco Quagliuolo (0622702412) - f.quagliuolo@studenti.unisa.it
#   - Giuseppe Alfonso Mangiola (0622702372) - g.mangiola1@studenti.unisa.it
# Canale: IZ
#
# Descrizione: Script per compilare e eseguire il job Hadoop MapReduce
#              sul cluster Docker del corso.
#
# NOTA: Eseguire questo script dal container MASTER:
#       docker exec -it master bash
#       cd /data
#       chmod +x scripts/run_mapreduce.sh
#       ./scripts/run_mapreduce.sh
# ============================================================================

echo "=============================================="
echo "  Big Data 2023/2024 - Esecuzione MapReduce"
echo "=============================================="

# ----------------------------------------------------------------------------
# CONFIGURAZIONE
# ----------------------------------------------------------------------------
WORKDIR="/data"
DATASET_LOCAL="$WORKDIR/dataset/customer.csv"
DATASET_HDFS="hdfs:///input/customer.csv"
OUTPUT_HDFS="hdfs:///output_mapreduce"
TEMP_HDFS="hdfs:///tmp/customer_temp"
JAR_NAME="CustomerSpending.jar"
MAIN_CLASS="mapreduce.CustomerDriver"

cd $WORKDIR

# ----------------------------------------------------------------------------
# STEP 1: Compilazione del codice Java
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 1: Compilazione codice Java..."

# Crea cartella build se non esiste
rm -rf build
mkdir -p build

# Compila tutti i file Java del package mapreduce
javac -encoding UTF-8 -cp "$(hadoop classpath)" -d build $(find src/mapreduce -name "*.java")

if [ $? -ne 0 ]; then
    echo "ERRORE: Compilazione fallita!"
    exit 1
fi
echo "Compilazione completata con successo."

# ----------------------------------------------------------------------------
# STEP 2: Creazione del JAR
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 2: Creazione archivio JAR..."
jar -cvf $JAR_NAME -C build .

if [ $? -ne 0 ]; then
    echo "ERRORE: Creazione JAR fallita!"
    exit 1
fi
echo "JAR creato: $JAR_NAME"

# ----------------------------------------------------------------------------
# STEP 3: Caricamento dataset su HDFS
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 3: Caricamento dataset su HDFS..."

# Crea directory input se non esiste
hdfs dfs -mkdir -p /input

# Carica o sovrascrive il dataset
hdfs dfs -put -f $DATASET_LOCAL $DATASET_HDFS
echo "Dataset caricato su HDFS: $DATASET_HDFS"

# ----------------------------------------------------------------------------
# STEP 4: Pulizia output precedente
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 4: Pulizia output precedente..."

# Rimuove output precedente se esiste
hdfs dfs -rm -r -f $OUTPUT_HDFS 2>/dev/null
hdfs dfs -rm -r -f $TEMP_HDFS 2>/dev/null
echo "Pulizia completata."

# ----------------------------------------------------------------------------
# STEP 5: Esecuzione Job MapReduce
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 5: Esecuzione Job MapReduce..."
echo "Input:  $DATASET_HDFS"
echo "Output: $OUTPUT_HDFS"
echo ""

hadoop jar $JAR_NAME $MAIN_CLASS $DATASET_HDFS $OUTPUT_HDFS

if [ $? -ne 0 ]; then
    echo "ERRORE: Esecuzione MapReduce fallita!"
    exit 1
fi

# ----------------------------------------------------------------------------
# STEP 6: Visualizzazione risultati
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 6: Visualizzazione risultati..."
echo ""
echo "=== RISULTATO (Classifica Spesa per Education - Decrescente) ==="
hdfs dfs -cat $OUTPUT_HDFS/part-r-00000

# ----------------------------------------------------------------------------
# STEP 7: Download output in locale
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 7: Download output in locale..."

# Crea cartella output locale se non esiste
mkdir -p $WORKDIR/output_mapreduce

# Scarica l'output
hdfs dfs -get -f $OUTPUT_HDFS/* $WORKDIR/output_mapreduce/

echo ""
echo "=============================================="
echo "  ESECUZIONE COMPLETATA CON SUCCESSO!"
echo "=============================================="
echo ""
echo "Output HDFS:   $OUTPUT_HDFS"
echo "Output locale: $WORKDIR/output_mapreduce/"
echo ""
