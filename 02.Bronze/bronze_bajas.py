from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

# -----------------------------
# Params (puedes pasarlos por argumentos)
# -----------------------------
RAW_PATH   = sys.argv[1] if len(sys.argv) > 1 else "01.RAW/reporte_pago_comisiones_bajas.tsv"
BRONZE_BASE= sys.argv[2] if len(sys.argv) > 2 else "02.Bronze/bajas_retenciones"
PERIODO    = sys.argv[3] if len(sys.argv) > 3 else "2025-03"  # YYYY-MM (mes de cálculo)

# -----------------------------
# 1) Leer RAW (TSV)
# -----------------------------
df = (
    spark.read
    .option("header", "true")
    .option("sep", "\t")              # tu archivo viene tabulado
    .option("encoding", "UTF-8")      # si fuera Latin-1 lo cambiamos
    .csv(RAW_PATH)
)

# -----------------------------
# 2) Normalización mínima de nombres (opcional pero recomendable)
#    (sin cambiar estructura de negocio: solo sanitiza espacios)
# -----------------------------
def clean_col(c: str) -> str:
    return c.strip().replace(" ", "_")

df = df.toDF(*[clean_col(c) for c in df.columns])

# -----------------------------
# 3) Columnas técnicas (auditoría)
# -----------------------------
df = (
    df
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_periodo", F.lit(PERIODO))
)

# Parse FECHA y HORA (en tu muestra FECHA=dd/MM/yyyy y HORA=HH:mm) :contentReference[oaicite:1]{index=1}
df = (
    df
    .withColumn("_fecha_evt", F.to_date(F.col("FECHA"), "dd/MM/yyyy"))
    .withColumn("_hora_evt", F.to_timestamp(F.concat_ws(" ", F.col("FECHA"), F.col("HORA")), "dd/MM/yyyy HH:mm"))
)

# Hash de registro para auditoría (estable ante re-cargas)
df = df.withColumn(
    "_record_hash",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns if not c.startswith("_")]), 256)
)

# -----------------------------
# 4) BRONZE HISTÓRICO (append-only)
#    Guarda cada ejecución y conserva todo para auditoría/seguridad
# -----------------------------
bronze_hist_path = f"{BRONZE_BASE}/historico"
(
    df.write
      .mode("append")
      .partitionBy("_periodo")     # partición mensual (500k/mes ok)
      .parquet(bronze_hist_path)
)

# -----------------------------
# 5) BRONZE SNAPSHOT FULL (overwrite del periodo)
#    "Foto" del mes actual (full load por cada corrida)
# -----------------------------
bronze_snap_path = f"{BRONZE_BASE}/snapshot"
(
    df.write
      .mode("overwrite")
      .partitionBy("_periodo")
      .parquet(bronze_snap_path)
)

# -----------------------------
# 6) Dataset VIGENTE del periodo (dedup por TELF_CLIENTE + FECHA)
#    Regla: duplicado se detecta por telefono y fecha.
#    Si hay múltiples en el día, me quedo con la última por hora.
# -----------------------------
w = Window.partitionBy(F.col("TELF_CLIENTE"), F.col("_fecha_evt"), F.col("_periodo")).orderBy(F.col("_hora_evt").desc_nulls_last())

df_vigente = (
    df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

bronze_vigente_path = f"{BRONZE_BASE}/vigente"
(
    df_vigente.write
      .mode("overwrite")
      .partitionBy("_periodo")
      .parquet(bronze_vigente_path)
)

print(f"OK -> historico: {bronze_hist_path}")
print(f"OK -> snapshot : {bronze_snap_path}")
print(f"OK -> vigente  : {bronze_vigente_path}")