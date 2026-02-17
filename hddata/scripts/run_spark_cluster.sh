#!/bin/bash
# ============================================================================
# PROGETTO BIG DATA 2023/2024 - Script Esecuzione Spark (Cluster)
# ============================================================================
# 
# Studenti:
#   - Francesco Quagliuolo (0622702412) - f.quagliuolo@studenti.unisa.it
#   - Giuseppe Alfonso Mangiola (0622702372) - g.mangiola1@studenti.unisa.it
# Canale: IZ
#
# Descrizione: Script per eseguire il job Spark in modalita cluster
#              sul cluster Docker del corso.
#
# NOTA: Eseguire questo script dal container MASTER:
#       docker exec -it master bash
#       cd /data
#       chmod +x scripts/run_spark_cluster.sh
#       ./scripts/run_spark_cluster.sh
# ============================================================================

echo "=============================================="
echo "  Big Data 2023/2024 - Esecuzione Spark"
echo "  (Modalita: LOCALE con java -cp)"
echo "=============================================="

# ----------------------------------------------------------------------------
# CONFIGURAZIONE
# ----------------------------------------------------------------------------
WORKDIR="/data"
DATASET_HDFS="hdfs://master:54310/input/customer.csv"
OUTPUT_HDFS="hdfs://master:54310/output_spark_cluster"
JAR_NAME="CustomerSpark.jar"
MAIN_CLASS="spark.SparkDriver"

cd $WORKDIR

# ----------------------------------------------------------------------------
# STEP 1: Verifica JAR esistente
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 1: Verifica JAR..."

if [ ! -f "$JAR_NAME" ]; then
    echo "JAR non trovato. Compilo il codice..."
    
    rm -rf build_spark
    mkdir -p build_spark
    javac -encoding UTF-8 -cp "$WORKDIR/spark_libs/*" -d build_spark $(find src/spark -name "*.java")
    
    if [ $? -ne 0 ]; then
        echo "ERRORE: Compilazione fallita!"
        exit 1
    fi
    
    jar -cvf $JAR_NAME -C build_spark .
fi
echo "JAR pronto: $JAR_NAME"

# ----------------------------------------------------------------------------
# STEP 2: Pulizia output precedente
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 2: Pulizia output precedente..."

hdfs dfs -rm -r -f /output_spark_cluster 2>/dev/null
echo "Pulizia completata."

# ----------------------------------------------------------------------------
# STEP 3: Esecuzione Spark
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 3: Esecuzione Spark..."
echo "Input:    $DATASET_HDFS"
echo "Output:   $OUTPUT_HDFS"
echo ""

SPARK_CP=$(echo $WORKDIR/spark_libs/*.jar | tr ' ' ':')

java -cp "$WORKDIR/$JAR_NAME:$SPARK_CP" \
    -Dspark.master=local[*] \
    -Dspark.app.name=WebConversionRate \
    -Dspark.hadoop.fs.defaultFS=hdfs://master:54310 \
    $MAIN_CLASS \
    $DATASET_HDFS \
    $OUTPUT_HDFS

if [ $? -ne 0 ]; then
    echo "ERRORE: Esecuzione Spark fallita!"
    exit 1
fi

echo ""
echo "=============================================="
echo "  ESECUZIONE COMPLETATA!"
echo "=============================================="
echo ""
echo "Output HDFS: $OUTPUT_HDFS"
echo ""
echo "Per vedere i risultati:"
echo "  hdfs dfs -cat /output_spark_cluster/part-*"
echo ""
