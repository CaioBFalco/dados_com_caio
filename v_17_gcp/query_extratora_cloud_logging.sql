-- AUTOR: CAIO FALCO (DADOS COM CAIO)
-- DATA: 25/09/2022

SELECT
  SUBSTR(CAST(TIMESTAMP_SUB(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, INTERVAL 3 HOUR) AS STRING),0,19)  AS starttime,
  SUBSTR(CAST(TIMESTAMP_SUB(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime, INTERVAL 3 HOUR) AS STRING),0,19)  AS endtime,
  SUBSTR(CAST(TIMESTAMP_SUB(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, INTERVAL 3 HOUR) AS STRING),11,9)  AS starthourminute,
  SUBSTR(CAST(TIMESTAMP_SUB(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime, INTERVAL 3 HOUR) AS STRING),11,9)  AS endhourminute,
  SUBSTR(protopayload_auditlog.authenticationInfo.principalEmail,0,INSTR(protopayload_auditlog.authenticationInfo.principalEmail,'@')-1) AS analyst,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS query,
  SUBSTR(protopayload_auditlog.status.message,0,120) AS message,
  protopayload_auditlog.status.CODE AS code,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.statementType AS job_type,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalTablesProcessed AS total_tables_processed,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.queryOutputRowCount AS queryoutputrowCount,
  ROUND(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/POW(10,9),2) as totalgigabytesbilled,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables,
  severity
FROM
  `[seu_projeto].[dataset].logging_ds.cloudaudit_googleapis_com_data_access`
WHERE LOWER(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query) NOT LIKE '%cloudaudit_googleapis_com_data_access%'
  ORDER BY TIMESTAMP DESC

--NÃO LEVA EM CONSIDERAÇÃO AS QUERIES SOBRE A PRÓPRIA TABELA DE LOG.  