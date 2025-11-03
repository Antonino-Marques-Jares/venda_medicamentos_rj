# Venda de medicamentos no Estado do Rio de Janeiro

## Esse conjunto de dados apresenta a listagem de medicamentos registrados na Anvisa entre os anos 2014 a 2021 e suas respectivas restrições de comercialização. 
[venda-de-medicamentos-controlados-e-antimicrobianos](https://dados.gov.br/dados/conjuntos-dados/venda-de-medicamentos-controlados-e-antimicrobianos---medicamentos-industrializados)

## Primeiro obstáculo para conseguir fazer este gráfico foi a quantidade de registros
Somando todas as linhas dos 95 arquivos csv's teríamos quase meio Bilhão de linhas na tabela o que se mostrou inviável dentro dos meus recursos disponíveis.  
Apesar de ser possível inserir  meio Bilhão de linhas em uma tabela MySQL, quando for realizar uma consulta provavelmente não vai concluir a não ser que faça paginação (fazer a consulta em partes menores).
Portanto decidi não analisar todo o Brasil e me restringir a pesquisa ao Estado do Rio de Janeiro.



# Curiosidade
O filme **Império da Dor na NetFlix** mostrou um caso de uma agente federal que percebeu o aumento esponencial de um determinado remédio controlado nos Estados Unidos, o que a levou a uma investigação em campo.

# Objetivo deste projeto
Levantamento científico para estudantes de medicina, para que possam associar o aumento de doenças relacionando o aumento de uso de drogas (lícitas ou não lícitas) à Municípios próximos a hospitais e clínicas.

### Passo 1 - baixar as 95 planilhas 
[ARQUIVOS](https://dados.gov.br/dados/conjuntos-dados/venda-de-medicamentos-controlados-e-antimicrobianos---medicamentos-industrializados)

### Passo 2 - Criar a Tabela DDL
CREATE TABLE `vendas_medicamentos` (
  `indice_global` bigint NOT NULL AUTO_INCREMENT,
  `ANO_VENDA` int DEFAULT NULL,
  `MES_VENDA` int DEFAULT NULL,
  `PRINCIPIO_ATIVO` varchar(255) DEFAULT NULL,
  `MUNICIPIO_VENDA` varchar(100) DEFAULT NULL,
  `QTD_VENDIDA` int DEFAULT NULL,
  `CONSELHO_PRESCRITOR` varchar(50) DEFAULT NULL,
  `UF_CONSELHO_PRESCRITOR` varchar(2) DEFAULT NULL,
  PRIMARY KEY (`indice_global`),
  KEY `idx_groupby_principal` (`ANO_VENDA`,`MES_VENDA`,`MUNICIPIO_VENDA`,`PRINCIPIO_ATIVO`),
  KEY `idx_ano_mes` (`ANO_VENDA`,`MES_VENDA`),
  KEY `idx_municipio` (`MUNICIPIO_VENDA`),
  KEY `idx_principio` (`PRINCIPIO_ATIVO`)
)

### Passo 3 Inserir os registros
Com python monte um código que leia os arquivos csv's no diretório e insira linha a linha na tabela vendas_medicamentos
No meu caso inseria apenas se UF_VENDA = 'RJ' pois restringi a pesquisa ao Estado do Rio de Janeiro.

## Passo 4 Escrevi um post sobre o ajuste no MySQL para conseguir obter resultados desta tabela
https://www.areadetrampo.com.br/quando-sua-tabela-tem-30-milhoes-de-registros-no-mysql/

### Passo 5 Fazer o agrupamento
Na minha tabela só tenho registros do Estado do Rio de Janeiro por este motivo não tenho na tabela UF_VENDA
Fiz uma lista com os Municípios do Rio de Janeiro e percorro cada item da lista e executo :
SELECT 
    ANO_VENDA, 
    MUNICIPIO_VENDA, 
    PRINCIPIO_ATIVO,
    SUM(QTD_VENDIDA) as TOTAL_VENDIDO
FROM trampo.vendas_medicamentos  
WHERE MUNICIPIO_VENDA = [MUNICÍPIO]
GROUP BY ANO_VENDA, MUNICIPIO_VENDA, PRINCIPIO_ATIVO;
Ao limitar a consulta acima por um município conseguimos obter este resultado e salvar em um dataset pandas e repetimos isso para os 92 Municípios do Estado do RJ.

### Passo 6 Salvar o resultado em um JSON ou CSV
Com o JSON podemos criar os gráficos com ajuda do JavaScript em um HTML.
Se preferir pode salvar em um csv e visualizar pelo PowerBI.







