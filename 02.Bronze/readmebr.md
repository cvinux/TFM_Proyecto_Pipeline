# Capa Bronze (02.Bronze)

Esta carpeta contiene los **artefactos de ingesta hacia la capa Bronze** del pipeline del TFM.

La capa **Bronze** representa la zona de almacenamiento **"tal cual" (as-is)**, donde se persisten los datos
provenientes de la fuente original con **mínima intervención**, conservando la trazabilidad y permitiendo auditoría.

---

## Fuente implementada

- **Fuente:** `reporte_pago_comisiones_bajas`
- **Tipo de carga:** FULL por ejecución (snapshot del periodo)
- **Acumulación histórica:** Sí (append-only) para auditoría y seguridad
- **Formato de salida:** Parquet
- **Deduplicación (vigencia del periodo):** por `TELF_CLIENTE` + `FECHA`
- **Volumen estimado:** ~500K registros / mes

---

## Principios de diseño

1. **No transformación de negocio (as-is):**  
   No se alteran reglas de cálculo ni semántica del dominio. Solo se añaden columnas técnicas de control.

2. **Trazabilidad:**  
   Se incorporan columnas técnicas para identificar el origen y el momento de ingesta.

3. **Auditoría y seguridad:**  
   Además del snapshot (FULL), se mantiene un histórico incremental de cada ejecución.

4. **Base para Silver/Gold:**  
   Bronze sirve como respaldo “confiable” para posteriores etapas de limpieza, estandarización y modelado.

---

## Salidas generadas (por fuente)

Para la fuente `reporte_pago_comisiones_bajas` se generan tres datasets en Bronze:

### 1) Histórico (append-only)
- **Ruta sugerida:** `02.Bronze/reporte_pago_comisiones_bajas/historico/`
- **Descripción:** mantiene todas las ejecuciones para auditoría.
- **Particionado:** por periodo (`_periodo`, formato YYYY-MM)

### 2) Snapshot del periodo (overwrite)
- **Ruta sugerida:** `02.Bronze/reporte_pago_comisiones_bajas/snapshot/`
- **Descripción:** representa la “foto” completa del periodo cargado (FULL).
- **Particionado:** por `_periodo`

### 3) Vigente del periodo (overwrite con deduplicación)
- **Ruta sugerida:** `02.Bronze/reporte_pago_comisiones_bajas/vigente/`
- **Descripción:** dataset preparado para consumo posterior, deduplicado por `TELF_CLIENTE` + `FECHA`.
- **Criterio de desempate:** se conserva el último registro por hora del evento cuando existan múltiples ocurrencias.
- **Particionado:** por `_periodo`

---

## Columnas técnicas incorporadas

- `_ingestion_ts` : timestamp de carga
- `_source_file`  : nombre/ruta del archivo fuente
- `_periodo`      : periodo de cálculo (YYYY-MM)
- `_fecha_evt`    : fecha del evento parseada desde `FECHA`
- `_hora_evt`     : timestamp combinado desde `FECHA` y `HORA`
- `_record_hash`  : hash del registro (auditoría/reproducibilidad)

> Las columnas de negocio se preservan "tal cual" se reciben desde la fuente.

---

## Nota sobre datos en el repositorio

Por confidencialidad y buenas prácticas, este repositorio **no incluye datos reales** ni resultados de ejecución.
Los archivos RAW y salidas (Parquet) deben mantenerse fuera del control de versiones mediante `.gitignore`.