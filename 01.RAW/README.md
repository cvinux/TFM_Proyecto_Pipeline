# Capa RAW (01.RAW)

La capa **RAW** corresponde a la zona de aterrizaje (*landing zone*) del pipeline de datos.

En esta capa se almacenan los archivos **exactamente como son recibidos desde la fuente**, sin aplicar
transformaciones, tipado ni reglas de negocio.

---

## Principios de la capa RAW

- Preservación del archivo original
- No modificación del contenido
- Organización por fuente y periodo
- Base para trazabilidad y auditoría

---

## Fuente implementada

- **Fuente:** reporte_pago_comisiones_bajas
- **Formato original:** TSV (tabulado)
- **Frecuencia:** mensual
- **Volumen estimado:** ~500K registros / mes
- **Tipo de carga:** FULL por periodo

---

## Estructura de almacenamiento

```text
01.RAW/reporte_pago_comisiones_bajas/
└── YYYY/
    └── MM/
        └── reporte_pago_comisiones_bajas.tsv
