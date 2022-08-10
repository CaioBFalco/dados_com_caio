SELECT
  cloud_sql.*,
  planilha.* EXCEPT (cliente_id)
FROM
  EXTERNAL_QUERY("projects/projeto-gcp-356114/locations/us-central1/connections/dados-com-caio-cnx",
    "SELECT * FROM caio_db.cliente;") cloud_sql
INNER JOIN
  `projeto-gcp-356114.meu_dataset.tb_ext_end_cli` planilha
ON
  cloud_sql.cliente_id = planilha.cliente_id;
  