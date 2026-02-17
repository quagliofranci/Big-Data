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
# NOTA: Eseguire questo script dal container SPARK-MASTER:
#       docker exec -it spark-master bash
#       cd /submit
#       chmod +x scripts/run_spark_cluster.sh
#       ./scripts/run_spark_cluster.sh
# ============================================================================

echo "=============================================="
echo "  Big Data 2023/2024 - Esecuzione Spark"
echo "  (Modalita: SPARK STANDALONE CLUSTER)"
echo "=============================================="

# ----------------------------------------------------------------------------
# CONFIGURAZIONE
# ----------------------------------------------------------------------------
WORKDIR="/submit"
DATASET_HDFS="hdfs://namenode:8020/input/customer.csv"
OUTPUT_HDFS="hdfs://namenode:8020/output_spark_cluster"
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
    javac -encoding UTF-8 -cp "/spark/jars/*" -d build_spark $(find src/spark -name "*.java")
    
    if [ $? -ne 0 ]; then
        echo "ERRORE: Compilazione fallita!"
        exit 1
    fi
    
    jar -cvf $JAR_NAME -C build_spark .
fi
echo "JAR pronto: $JAR_NAME"

# ----------------------------------------------------------------------------
# STEP 2: Esecuzione Spark su Cluster
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 2: Esecuzione Spark su Cluster..."
echo "Modalita: spark://spark-master:7077"
echo "Input:    $DATASET_HDFS"
echo "Output:   $OUTPUT_HDFS"
echo ""

/spark/bin/spark-submit \
    --class $MAIN_CLASS \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 512m \
    --total-executor-cores 2 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
    $WORKDIR/$JAR_NAME \
    $DATASET_HDFS \
    $OUTPUT_HDFS

if [ $? -ne 0 ]; then
    echo "ERRORE: Esecuzione Spark fallita!"
    exit 1
fi

echo ""
echo "=============================================="
echo "  ESECUZIONE CLUSTER COMPLETATA!"
echo "=============================================="
echo ""
echo "Output HDFS: $OUTPUT_HDFS"
echo ""
echo "Per vedere i risultati, esegui dal container namenode:"
echo "  hdfs dfs -cat /output_spark_cluster/part-*"
echo ""
