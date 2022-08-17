####################################################################################
## AUTOR: CAIO FALCO (CANAL DADOS COM CAIO)
## DATA: 11/08/2022
## DESCRICAO: SCRIPT QUE EXPORTA UMA TABELA PARA O GCS GERADA A PARTIR DE UMA CONSULTA NO BIGQUERY 
## IDENTIFICANDO OS 5 MAIORES CLIENTES (MAIORES COMPRADORES NO DIA ANTERIOR)
####################################################################################

export refdate=`date -d '-1 day' '+%Y%m%d'` #A TABELA GERADA TERA A DATA DE REFERENCIA DOS COMPRADORES
export data_hora=`date '+%Y-%m-%d'_%H:%M:%S` #DATA E HORÁRIO PARA O NOME DO ARQUIVO DE LOG


mkdir -p /home/caiobfalco/log/ #GERA O DIRETÓRIO ONDE SERÃO ARMAZENADOS OS LOGS DE EXECUÇÃO
{
echo ""
echo "Execução iniciada as ${data_hora}"
echo ""
echo "Gerando a tabela projeto-gcp-356114.meu_dataset.tb_maiores_cli_${refdate}:"

bq query 	--use_legacy_sql=false \
			--project_id=projeto-gcp-356114 \
'
CREATE OR REPLACE TABLE `projeto-gcp-356114.meu_dataset.tb_maiores_cli_'${refdate}'` AS 
SELECT
  cons.COD,
  cons.NOME,
  ROUND(SUM(VALOR_COMPRA),1) AS TT_COMPRAS
FROM
  `projeto-gcp-356114.meu_dataset.tb_pes_consolidado` cons
INNER JOIN
  `projeto-gcp-356114.meu_dataset.tb_pes_compras` comp
ON
  cons.COD = comp.COD
WHERE DATA_COMPRA = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY
  COD,
  NOME
ORDER BY
  TT_COMPRAS DESC
LIMIT
  5
'
retorno=$?
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha na etapa de criação da tabela com os maiores clientes. Consultar log para mais detalhes."
	echo "Execução mal sucedida. Encerrando execução."
	echo ""
	exit 1;
fi

echo "Tabela com os maiores clientes para o dia ${refdate} gerada com sucesso."
echo ""
echo "Iniciando exportação da tabela projeto-gcp-356114.meu_dataset.tb_maiores_cli_${refdate} para o GCS."
bq extract	--destination_format=CSV \
			--project_id=projeto-gcp-356114 \
       		--field_delimiter=',' \
			--compression=NONE \
			--print_header=true \
			projeto-gcp-356114:meu_dataset.tb_maiores_cli_${refdate} \
			gs://meu-bucket-gcs/v10/tb_maiores_cli_${refdate}.csv
retorno=$?
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha na etapa de exportação da tabela para o GCS. Consultar log para mais detalhes."
	echo "Execução mal sucedida. Encerrando execução."
	echo ""
	exit 1;
fi

echo ""
echo "Exportação realizada com sucesso."
echo ""
echo "Dropando a tabela meu_dataset.tb_maiores_cli_${refdate}."
bq rm 	-f -t meu_dataset.tb_maiores_cli_${refdate}
retorno=$?
if [ ${retorno} -ne 0 ]; then
	echo "Falha ao excluir a tabela tb_maiores_cli_${refdate}."
	echo ""
else
	echo "Tabela tb_maiores_cli_${refdate} dropada com sucesso."
fi

echo ""
echo "Pasta de destino no GCS: /meu-bucket-gcs/v10/"
echo "Arquivo de destino: tb_maiores_cli_${refdate}.csv"
echo ""
echo "Execução concluída com sucesso as `date '+%Y-%m-%d'_%H:%M:%S`."
echo ""

} > /home/caiobfalco/log/export_tb_maiores_cli_${data_hora}.log
