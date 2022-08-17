####################################################################################
## AUTOR: CAIO FALCO
## DATA: 11/08/2022
## DESCRICAO: SCRIPT QUE IDENTIFICA TODAS AS TABELAS COM "OLD" NO DATASET E ENTÃO AS DROPA.
####################################################################################

export data_hora=`date '+%Y-%m-%d'_%H:%M:%S` #DATA E HORÁRIO PARA O NOME DO ARQUIVO DE LOG
export refdate=`date '+%Y%m%d'`
mkdir -p /home/caiobfalco/log/
{

echo ""
echo "EXECUÇÃO INICIADA AS `date '+%Y-%m-%d'_%H:%M:%S`"
echo ""
echo ""
echo "ETAPA 01 - IDENTIFICAÇÃO DAS TABELAS A SEREM DELETADAS. EM ANDAMENTO."

bq query 	--use_legacy_sql=false \
			--project_id=projeto-gcp-356114 \
'
CREATE OR REPLACE TABLE `projeto-gcp-356114.meu_dataset.tb_lista_tabelas_'${refdate}'` AS 
SELECT
  TABLE_ID 
FROM
  `projeto-gcp-356114.meu_dataset.__TABLES__`
WHERE TABLE_ID LIKE "%_old_%"
'
retorno=$?
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha na etapa de levantamento das tabelas a serem dropadas. Consultar log para mais detalhes."
	echo "Execução mal sucedida. Encerrando execução."
	echo ""
	exit 1;
fi

echo "ETAPA 01 - IDENTIFICACAO DAS TABELAS CONCLUÍDA COM SUCESSO."
echo "ETAPA 02 - EXPORT DA LISTA DE TABELAS PARA O GCS. EM ANDAMENTO."

echo ""
echo "INICIANDO EXPORTAÇÃO DA TABELA projeto-gcp-356114.meu_dataset.tb_lista_tabelas_${refdate} para o GCS."
bq extract	--destination_format=CSV \
			--project_id=projeto-gcp-356114 \
       		--field_delimiter=',' \
			--compression=NONE \
			--print_header=false \
			projeto-gcp-356114:meu_dataset.tb_lista_tabelas_${refdate} \
			gs://meu-bucket-gcs/v11/tb_lista_tabelas_${refdate}.csv
retorno=$?
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha na etapa de exportação da tabela para o GCS. Consultar log para mais detalhes."
	echo "Execução mal sucedida. Encerrando execução."
	echo ""
	exit 1;
fi

echo "ETAPA 02 - EXPORT DA LISTA DE TABELAS PARA O GCS CONCLUÍDO COM SUCESSO."
echo "ETAPA 03 - COPIANDO ARQUIVO DO BUCKET PARA O CLOUD SHELL. EM ANDAMENTO."
echo ""

mkdir -p /home/caiobfalco/param/
gsutil cp gs://meu-bucket-gcs/v11/tb_lista_tabelas_${refdate}.csv /home/caiobfalco/param/
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha na etapa de cópia do arquivo de parâmetro para a máquina do Cloud Shell. Consultar log para mais detalhes."
	echo "Execução mal sucedida. Encerrando execução."
	echo ""
	exit 1;
fi

echo "ETAPA 03 - CÓPIA DO ARQUIVO DE PARÂMETRO DO BUCKET PARA O CLOUD SHELL REALIZADO COM SUCESSO."
echo "ETAPA 04 - DROP DAS TABELAS DO ARQUIVO DE PARÂMETRO. EM ANDAMENTO."
echo ""

arq_param=/home/caiobfalco/param/tb_lista_tabelas_${refdate}.csv
echo "DROPANDO TABELAS NO DATASET: meu_dataset"
echo "UTILIZANDO O ARQUIVO DE PARÂMETRO tb_lista_tabelas_${refdate}.csv"
echo ""

contador=0
while read tabela 
do
bq rm 	-f -t meu_dataset.${tabela}
retorno=$?
if [ ${retorno} -ne 0 ]; then
	echo "Falha ao excluir a tabela ${tabela}."
else
	echo "Tabela ${tabela} excluída com sucesso."
	contador=$((contador+1))
fi
done < ${arq_param}

echo ""
echo "QUANTIDADE DE TABELAS EXCLUÍDAS: ${contador}."
echo ""
echo "ETAPA 04 - TABELAS DROPADAS COM SUCESSO."
echo "ETAPA 05 - DROPAR A TABELA COM A LISTA. EM ANDAMENTO."
bq rm 	-f -t meu_dataset.tb_lista_tabelas_${refdate}

retorno=$?
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha ao excluir a tabela tb_lista_tabelas_${refdate}."
	echo ""
else
	echo ""
	echo "ETAPA 05 - TABELA DROPADA COM SUCESSO."
fi 

echo "ETAPA 06 - EXCLUSÃO ARQUIVO DE PARÂMETRO DO BUCKET E DO CLOUD SHELL. EM ANDAMENTO."
echo ""
gsutil rm gs://meu-bucket-gcs/v11/tb_lista_tabelas_${refdate}.csv 
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha na etapa de deleção do arquivo de parâmetro do bucket. Consultar log para mais detalhes."
	echo "Execução mal sucedida. Encerrando execução."
	echo ""
	exit 1;
fi

rm /home/caiobfalco/param/tb_lista_tabelas_${refdate}.csv
if [ ${retorno} -ne 0 ]; then
	echo ""
	echo "Falha na etapa de deleção do arquivo de parâmetro do Cloud Shell. Consultar log para mais detalhes."
	echo "Execução mal sucedida. Encerrando execução."
	echo ""
	exit 1;
fi

echo "ETAPA 06 - ARQUIVOS EXCLUÍDOS DO BUCKET E DO CLOUD SHELL COM SUCESSO!!!"
echo ""
echo ""
echo "TODAS AS ETAPAS FORAM CONCLUÍDAS COM SUCESSO. FIM DE EXECUÇÃO AS  `date '+%Y-%m-%d'_%H:%M:%S`."
echo ""

} > /home/caiobfalco/log/drop_table_meu_dataset_${data_hora}.log
