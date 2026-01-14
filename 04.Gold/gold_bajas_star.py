from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys

# -----------------------------
# Params
# -----------------------------
SILVER_BASE = sys.argv[1] if len(sys.argv) > 1 else "03.Silver/reporte_pago_comisiones_bajas"
GOLD_BASE   = sys.argv[2] if len(sys.argv) > 2 else "04.Gold/reporte_pago_comisiones_bajas"
PERIODO     = sys.argv[3] if len(sys.argv) > 3 else "2025-03"  # YYYY-MM

SILVER_PATH = f"{SILVER_BASE}/curated"

DIM_FECHA_PATH   = f"{GOLD_BASE}/dim_fecha"
DIM_CLIENTE_PATH = f"{GOLD_BASE}/dim_cliente"
DIM_ENTIDAD_PATH = f"{GOLD_BASE}/dim_entidad"
DIM_CANAL_PATH   = f"{GOLD_BASE}/dim_canal"
FACT_PATH        = f"{GOLD_BASE}/fact_bajas_retenciones"

# -----------------------------
# Helpers
# -----------------------------
def sk(*cols):
    """Surrogate key determinística (hash) para reproducibilidad."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256)

def col_if_exists(df, name, default=None):
    return F.col(name) if name in df.columns else F.lit(default)

# -----------------------------
# 1) Read Silver
# -----------------------------
df = (
    spark.read.parquet(SILVER_PATH)
    .filter(F.col("_periodo") == F.lit(PERIODO))
)

# Validación mínima de campos esperados
required = ["telefono_cliente", "fecha_evento"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise Exception(f"Faltan columnas necesarias en Silver: {missing}")

# -----------------------------
# 2) Dim Fecha
# -----------------------------
dim_fecha = (
    df.select(F.col("fecha_evento").alias("fecha"))
      .where(F.col("fecha").isNotNull())
      .distinct()
      .withColumn("sk_fecha", sk(F.col("fecha")))
      .withColumn("anio", F.year("fecha"))
      .withColumn("mes", F.month("fecha"))
      .withColumn("dia", F.dayofmonth("fecha"))
      .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
      .withColumn("dia_semana", F.date_format("fecha", "E"))
      .withColumn("semana_anio", F.weekofyear("fecha"))
      .withColumn("_periodo", F.lit(PERIODO))
)

# -----------------------------
# 3) Dim Cliente (teléfono)
# -----------------------------
dim_cliente = (
    df.select(F.col("telefono_cliente").alias("telefono"))
      .where(F.col("telefono").isNotNull() & (F.col("telefono") != ""))
      .distinct()
      .withColumn("sk_cliente", sk(F.col("telefono")))
      .withColumn("_periodo", F.lit(PERIODO))
)

# -----------------------------
# 4) Dim Entidad (si existe)
# -----------------------------
# En Silver intentamos tener ruc_entidad, y opcionalmente entidad (string)
entidad_nombre = col_if_exists(df, "entidad", None)
dim_entidad = (
    df.select(
        col_if_exists(df, "ruc_entidad", None).alias("ruc_entidad"),
        entidad_nombre.alias("entidad")
    )
    .distinct()
    .withColumn("sk_entidad", sk(F.col("ruc_entidad"), F.col("entidad")))
    .withColumn("_periodo", F.lit(PERIODO))
)

# -----------------------------
# 5) Dim Canal (si existe)
# -----------------------------
canal_col = col_if_exists(df, "canal", None)
dim_canal = (
    df.select(canal_col.alias("canal"))
      .distinct()
      .withColumn("sk_canal", sk(F.col("canal")))
      .withColumn("_periodo", F.lit(PERIODO))
)

# -----------------------------
# 6) Fact: bajas/retenciones (a nivel evento deduplicado)
#    - Llave natural: telefono_cliente + fecha_evento + ts_evento (si existe)
#    - Métricas: montos si existen; si no, al menos conteo_evento=1
# -----------------------------
ts_evento = col_if_exists(df, "ts_evento", None)

# Montos posibles (según Silver generamos en minúsculas si existían en origen)
monto_total     = col_if_exists(df, "monto_total", None)
monto_operador  = col_if_exists(df, "monto_operador", None)
monto_rp        = col_if_exists(df, "monto_rp", None)
monto_retail    = col_if_exists(df, "monto_retail", None)
total           = col_if_exists(df, "total", None)

fact = (
    df
    .withColumn("sk_fecha",   sk(F.col("fecha_evento")))
    .withColumn("sk_cliente", sk(F.col("telefono_cliente")))
    .withColumn("sk_entidad", sk(col_if_exists(df, "ruc_entidad", None), entidad_nombre))
    .withColumn("sk_canal",   sk(canal_col))
    .withColumn("conteo_evento", F.lit(1))
    .withColumn("fact_id", sk(F.col("telefono_cliente"), F.col("fecha_evento"), ts_evento, F.lit(PERIODO)))
    .select(
        F.col("fact_id"),
        F.col("sk_fecha"),
        F.col("sk_cliente"),
        F.col("sk_entidad"),
        F.col("sk_canal"),
        F.col("fecha_evento").alias("fecha"),
        F.col("telefono_cliente").alias("telefono"),
        ts_evento.alias("ts_evento"),
        # Métricas (solo si existen, sino quedarán null)
        monto_total.alias("monto_total"),
        monto_operador.alias("monto_operador"),
        monto_rp.alias("monto_rp"),
        monto_retail.alias("monto_retail"),
        total.alias("total"),
        F.col("conteo_evento"),
        # Auditoría / trazabilidad
        F.col("_record_hash"),
        F.col("_source_file"),
        F.col("_ingestion_ts"),
        F.col("_periodo")
    )
)

# -----------------------------
# 7) Write Gold (overwrite por periodo)
# -----------------------------
# Dimensiones y fact por periodo (reproducible y fácil de recalcular)
(
    dim_fecha.write.mode("overwrite")
    .partitionBy("_periodo")
    .parquet(DIM_FECHA_PATH)
)

(
    dim_cliente.write.mode("overwrite")
    .partitionBy("_periodo")
    .parquet(DIM_CLIENTE_PATH)
)

(
    dim_entidad.write.mode("overwrite")
    .partitionBy("_periodo")
    .parquet(DIM_ENTIDAD_PATH)
)

(
    dim_canal.write.mode("overwrite")
    .partitionBy("_periodo")
    .parquet(DIM_CANAL_PATH)
)

(
    fact.write.mode("overwrite")
    .partitionBy("_periodo")
    .parquet(FACT_PATH)
)

print(f"OK -> dim_fecha   : {DIM_FECHA_PATH}")
print(f"OK -> dim_cliente : {DIM_CLIENTE_PATH}")
print(f"OK -> dim_entidad : {DIM_ENTIDAD_PATH}")
print(f"OK -> dim_canal   : {DIM_CANAL_PATH}")
print(f"OK -> fact        : {FACT_PATH}")