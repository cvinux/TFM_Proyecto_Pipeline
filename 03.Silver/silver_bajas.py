from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys
import re

# -----------------------------
# Parámetros
# -----------------------------
BRONZE_BASE = sys.argv[1] if len(sys.argv) > 1 else "02.Bronze/reporte_pago_comisiones_bajas"
SILVER_BASE = sys.argv[2] if len(sys.argv) > 2 else "03.Silver/reporte_pago_comisiones_bajas"
PERIODO     = sys.argv[3] if len(sys.argv) > 3 else "2025-03"  # YYYY-MM

BRONZE_VIGENTE_PATH  = f"{BRONZE_BASE}/vigente"
SILVER_PATH          = f"{SILVER_BASE}/curated"

# -----------------------------
# Helpers
# -----------------------------
def normalize_phone_col(colname: str):
    """
    Normaliza teléfonos:
    - deja solo dígitos
    - quita prefijo 51 si viene con 11 dígitos y empieza en 51
    - valida longitud final (9 típicamente en PE móvil, pero aquí solo marcamos flags)
    """
    c = F.regexp_replace(F.col(colname).cast("string"), r"[^0-9]", "")
    # si viene como 51######### (11 dígitos) => quitar 51
    c = F.when((F.length(c) == 11) & (F.substring(c, 1, 2) == F.lit("51")), F.substring(c, 3, 9)).otherwise(c)
    return c

def safe_decimal(colname: str, scale=2):
    """
    Convierte strings con coma/puntos a decimal:
    - reemplaza coma por punto
    - si está vacío => null
    """
    c = F.trim(F.col(colname).cast("string"))
    c = F.when((c == "") | c.isNull(), F.lit(None)).otherwise(c)
    c = F.regexp_replace(c, ",", ".")
    return c.cast(DecimalType(18, scale))

# -----------------------------
# 1) Leer Bronze (vigente)
# -----------------------------
df = (
    spark.read
    .parquet(BRONZE_VIGENTE_PATH)
    .filter(F.col("_periodo") == F.lit(PERIODO))
)

# -----------------------------
# 2) Limpieza base: trim en strings
# -----------------------------
# Aplica trim a todas las columnas string (sin tocar columnas técnicas)
for c, t in df.dtypes:
    if t == "string" and not c.startswith("_"):
        df = df.withColumn(c, F.trim(F.col(c)))

# -----------------------------
# 3) Tipado + estandarización específica
# -----------------------------
# FECHA y HORA ya venían parseadas en Bronze como _fecha_evt y _hora_evt,
# pero aquí las consolidamos como campos Silver "oficiales" del evento.
df = (
    df
    .withColumn("fecha_evento", F.col("_fecha_evt"))
    .withColumn("ts_evento", F.col("_hora_evt"))
)

# Normaliza teléfono (campo clave)
# En tu fuente aparece TELF_CLIENTE. :contentReference[oaicite:1]{index=1}
if "TELF_CLIENTE" in df.columns:
    df = df.withColumn("telefono_cliente", normalize_phone_col("TELF_CLIENTE"))
else:
    # fallback si el campo tuviera otro nombre
    df = df.withColumn("telefono_cliente", F.lit(None).cast(StringType()))

# Ejemplos de tipado numérico: adapta si tus columnas reales son distintas
# (en el txt hay columnas como MONTO_TOTAL, MONTO_OPERADOR, etc.) :contentReference[oaicite:2]{index=2}
for money_col in ["MONTO_TOTAL", "MONTO_OPERADOR", "MONTO_RP", "MONTO_RETAIL", "TOTAL"]:
    if money_col in df.columns:
        df = df.withColumn(money_col.lower(), safe_decimal(money_col, scale=2))

# Identificadores (mantener como string estandar)
for id_col in ["Ruc_Entidad", "RUC_ENTIDAD", "RUC", "Ruc"]:
    if id_col in df.columns:
        df = df.withColumn("ruc_entidad", F.regexp_replace(F.col(id_col).cast("string"), r"[^0-9]", ""))

# Texto: entidad/canal/estado/usuario, etc. (si existen)
for txt_col in ["ENTIDAD", "CANAL", "ESTADO", "USUARIO", "EJECUTIVO", "NOMBRE_CLIENTE", "CLIENTE"]:
    if txt_col in df.columns:
        df = df.withColumn(txt_col.lower(), F.col(txt_col))

# -----------------------------
# 4) Reglas técnicas / Quality flags (sin “cálculo negocio”)
# -----------------------------
df = (
    df
    .withColumn("dq_tel_no_nulo", F.col("telefono_cliente").isNotNull() & (F.col("telefono_cliente") != ""))
    .withColumn("dq_tel_len_9", F.length(F.col("telefono_cliente")) == 9)
    .withColumn("dq_fecha_valida", F.col("fecha_evento").isNotNull())
    .withColumn("dq_key_ok", F.col("dq_tel_no_nulo") & F.col("dq_fecha_valida"))
)

# Score simple de calidad (0-3)
df = df.withColumn(
    "dq_score",
    (F.col("dq_tel_no_nulo").cast("int") + F.col("dq_tel_len_9").cast("int") + F.col("dq_fecha_valida").cast("int"))
)

# -----------------------------
# 5) Dedup de seguridad (por si Bronze vigente no está aplicado en alguna corrida)
#    Llave: telefono + fecha + periodo (y se queda el último por ts_evento)
# -----------------------------
w = (
    Window
    .partitionBy("telefono_cliente", "fecha_evento", "_periodo")
    .orderBy(F.col("ts_evento").desc_nulls_last(), F.col("_ingestion_ts").desc_nulls_last())
)
df = (
    df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# -----------------------------
# 6) Selección final (curated)
#    Mantén columnas originales + derivadas + técnicas
# -----------------------------
# Dejamos:
# - originales (as-is)
# - telefono_cliente, fecha_evento, ts_evento
# - flags dq
# - columnas técnicas _...
curated_cols = df.columns  # mantenemos todo para trazabilidad en Silver
df_silver = df.select(*curated_cols)

# -----------------------------
# 7) Write Silver
# -----------------------------
(
    df_silver.write
    .mode("overwrite")
    .partitionBy("_periodo")
    .parquet(SILVER_PATH)
)

print(f"OK -> Silver curated: {SILVER_PATH} (periodo={PERIODO})")