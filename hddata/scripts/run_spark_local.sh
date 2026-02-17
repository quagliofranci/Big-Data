#!/bin/bash
# ============================================================================
# PROGETTO BIG DATA 2023/2024 - Script Esecuzione Spark
# ============================================================================
# 
# Studenti:
#   - Francesco Quagliuolo (0622702412) - f.quagliuolo@studenti.unisa.it
#   - Giuseppe Alfonso Mangiola (0622702372) - g.mangiola1@studenti.unisa.it
# Canale: IZ
#
# ESEGUIRE DAL CONTAINER NAMENODE:
#   docker exec -it namenode bash
#   cd /submit
#   chmod +x scripts/run_spark_local.sh
#   ./scripts/run_spark_local.sh
# ============================================================================

echo "=============================================="
echo "  Big Data 2023/2024 - Esecuzione Spark"
echo "=============================================="

WORKDIR="/submit"
cd $WORKDIR

# ----------------------------------------------------------------------------
# STEP 1: Compilazione
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 1: Compilazione codice Spark..."

rm -rf build_spark
mkdir -p build_spark

javac -encoding UTF-8 -cp "$WORKDIR/spark_libs/*" -d build_spark $(find src/spark -name "*.java")

if [ $? -ne 0 ]; then
    echo "ERRORE: Compilazione fallita!"
    exit 1
fi
echo "Compilazione completata."

# ----------------------------------------------------------------------------
# STEP 2: Creazione JAR
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 2: Creazione JAR..."

jar -cvf CustomerSpark.jar -C build_spark .
echo "JAR creato: CustomerSpark.jar"

# ----------------------------------------------------------------------------
# STEP 3: Pulizia output precedente
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 3: Pulizia output precedente..."

hdfs dfs -rm -r -f /output_spark 2>/dev/null
echo "Pulizia completata."

# ----------------------------------------------------------------------------
# STEP 4: Esecuzione Spark con Java
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 4: Esecuzione Spark..."
echo ""

# Costruisci il classpath con tutte le librerie Spark
SPARK_CP=$(echo $WORKDIR/spark_libs/*.jar | tr ' ' ':')

java -cp "$WORKDIR/CustomerSpark.jar:$SPARK_CP" \
    -Dspark.master=local[*] \
    -Dspark.app.name=WebConversionRate \
    -Dspark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
    spark.SparkDriver \
    hdfs://namenode:8020/input/customer.csv \
    hdfs://namenode:8020/output_spark

if [ $? -ne 0 ]; then
    echo ""
    echo "ERRORE: Esecuzione Spark fallita!"
    exit 1
fi

# ----------------------------------------------------------------------------
# STEP 5: Visualizzazione risultati
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 5: Risultati..."
echo ""
echo "=== WEB CONVERSION RATE PER ANNO ==="
hdfs dfs -cat /output_spark/part-*

# ----------------------------------------------------------------------------
# STEP 6: Salvataggio locale
# ----------------------------------------------------------------------------
echo ""
echo ">>> STEP 6: Salvataggio in locale..."

mkdir -p $WORKDIR/output_spark
hdfs dfs -get -f /output_spark/* $WORKDIR/output_spark/

echo ""
echo "=============================================="
echo "  COMPLETATO!"
echo "=============================================="
echo "Output: $WORKDIR/output_spark/"
echo ""
