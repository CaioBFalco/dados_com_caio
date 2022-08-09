--PRIMEIRO SQL PARA O MÓDULO II, VIDEO SOBRE METADADOS NO BQ
SELECT
  DATASET_ID,
  TABLE_ID,
  size_bytes,
  creation_time,
  last_modified_time,
  ROW_COUNT,
  type
FROM
  `sandbox-atra.sandbox_ds.__TABLES__`
ORDER BY
  size_bytes DESC