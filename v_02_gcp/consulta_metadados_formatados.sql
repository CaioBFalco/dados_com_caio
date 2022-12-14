-- AUTOR: CAIO FALCO (CANAL: DADOS COM CAIO)

SELECT
  DATASET_ID AS DATASET,
  TABLE_ID AS TABELA,
  -- CONVERTE BYTES PARA GB 
  ROUND(SIZE_BYTES/POW(10,9),2) AS SIZE_GB,
  -- CONVERTE TIMESTAMP UNIX PARA TIMESTAMP 
  TIMESTAMP_MILLIS(creation_time) AS DATA_CRIACAO,
  TIMESTAMP_MILLIS(last_modified_time) AS DATA_ULTIMA_MODIF,
  ROW_COUNT AS QTD_REG,
  CASE
    WHEN TYPE = 1 THEN 'Tabela'
    WHEN TYPE = 2 THEN 'View'
  ELSE
  NULL
END
  AS TYPE
FROM
  `sandbox-atra.sandbox_ds.__TABLES__`
ORDER BY
  SIZE_GB DESC