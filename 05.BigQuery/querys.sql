SELECT
  _periodo,
  COUNT(*) AS filas,
  COUNT(DISTINCT telefono_cliente) AS clientes_unicos,
  MIN(fecha_evento) AS fecha_min,
  MAX(fecha_evento) AS fecha_max
FROM `project.dataset.silver_reporte_pago_comisiones_bajas_curated`
GROUP BY _periodo
ORDER BY _periodo;

DECLARE p_periodo STRING DEFAULT '2025-03';

SELECT
  telefono_cliente,
  COUNT(*) AS bajas
FROM `project.dataset.silver_reporte_pago_comisiones_bajas_curated`
WHERE _periodo = p_periodo
GROUP BY telefono_cliente
ORDER BY bajas DESC
LIMIT 50;

DECLARE p_periodo STRING DEFAULT '2025-03';

SELECT
  f.fecha,
  COUNT(*) AS bajas,
  SUM(COALESCE(f.monto_total, 0)) AS monto_total
FROM `project.dataset.gold_fact_bajas_retenciones` f
WHERE f._periodo = p_periodo
GROUP BY f.fecha
ORDER BY f.fecha;

DECLARE p_periodo STRING DEFAULT '2025-03';

SELECT
  c.canal,
  COUNT(*) AS bajas,
  SUM(COALESCE(f.monto_total, 0)) AS monto_total
FROM `project.dataset.gold_fact_bajas_retenciones` f
LEFT JOIN `project.dataset.gold_dim_canal` c
  ON f.sk_canal = c.sk_canal AND c._periodo = f._periodo
WHERE f._periodo = p_periodo
GROUP BY c.canal
ORDER BY bajas DESC;

DECLARE p_periodo STRING DEFAULT '2025-03';

SELECT
  e.ruc_entidad,
  e.entidad,
  COUNT(*) AS bajas,
  SUM(COALESCE(f.monto_total, 0)) AS monto_total
FROM `project.dataset.gold_fact_bajas_retenciones` f
LEFT JOIN `project.dataset.gold_dim_entidad` e
  ON f.sk_entidad = e.sk_entidad AND e._periodo = f._periodo
WHERE f._periodo = p_periodo
GROUP BY e.ruc_entidad, e.entidad
ORDER BY bajas DESC
LIMIT 50;

DECLARE p_periodo STRING DEFAULT '2025-03';

SELECT
  _periodo,
  COUNT(*) AS bajas,
  SUM(COALESCE(monto_total, 0)) AS monto_total,
  AVG(COALESCE(monto_total, 0)) AS ticket_promedio
FROM `project.dataset.gold_fact_bajas_retenciones`
WHERE _periodo = p_periodo
GROUP BY _periodo;

DECLARE p_periodo STRING DEFAULT '2025-03';

SELECT
  _periodo,
  COUNT(*) AS total,
  SUM(CASE WHEN dq_key_ok THEN 1 ELSE 0 END) AS key_ok,
  ROUND(100 * SAFE_DIVIDE(SUM(CASE WHEN dq_key_ok THEN 1 ELSE 0 END), COUNT(*)), 2) AS pct_key_ok
FROM `project.dataset.silver_reporte_pago_comisiones_bajas_curated`
WHERE _periodo = p_periodo
GROUP BY _periodo;

DECLARE p_periodo STRING DEFAULT '2025-03';

SELECT
  telefono_cliente,
  fecha_evento,
  COUNT(*) AS repeticiones
FROM `project.dataset.silver_reporte_pago_comisiones_bajas_curated`
WHERE _periodo = p_periodo
GROUP BY telefono_cliente, fecha_evento
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC
LIMIT 100;


DECLARE p_periodo STRING DEFAULT '2025-03';

WITH s AS (
  SELECT COUNT(*) AS silver_rows
  FROM `project.dataset.silver_reporte_pago_comisiones_bajas_curated`
  WHERE _periodo = p_periodo
),
g AS (
  SELECT COUNT(*) AS gold_rows
  FROM `project.dataset.gold_fact_bajas_retenciones`
  WHERE _periodo = p_periodo
)
SELECT
  p_periodo AS periodo,
  s.silver_rows,
  g.gold_rows,
  (s.silver_rows - g.gold_rows) AS diff
FROM s, g;