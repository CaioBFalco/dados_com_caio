############################
## AUTOR: CAIO FALCO
## DATA: 11/08/2022
## DESCRICAO: SCRIPT QUE IDENTIFICA TODAS AS TABELAS COM "OLD" NO DATASET E ENTÃO AS DROPA.
############################

export data_hora=`date '+%Y-%m-%d'_%H:%M:%S` #DATA E HORÁRIO PARA O NOME DO ARQUIVO DE LOG
mkdir -p /home/caiobfalco/log/
{
echo "EXECUÇÃO INICIADA AS `date '+%Y-%m-%d'_%H:%M:%S`"
echo ""
echo ""
echo "CRIANDO TABELAS OLD."
echo ""
for i in  {1..20}; do
bq query 	--use_legacy_sql=false \
			--project_id=projeto-gcp-356114 \
'
CREATE OR REPLACE TABLE `projeto-gcp-356114.meu_dataset.tabela_old_'${i}'` AS 
SELECT
  * 
FROM
  `projeto-gcp-356114.meu_dataset.tb_pes_consolidado`
'
done

echo "TABELAS CRIADAS COM SUCESSO."
echo ""
echo ""
echo "EXECUÇÃO CONCLUÍDA COM SUCESSO AS `date '+%Y-%m-%d'_%H:%M:%S`."
echo ""

} > /home/caiobfalco/log/create_table_meu_dataset_${data_hora}.log
