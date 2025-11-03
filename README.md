# Venda de medicamentos no Estado do Rio de Janeiro

## Esse conjunto de dados apresenta a listagem de medicamentos registrados na Anvisa entre os anos 2014 a 2021 e suas respectivas restrições de comercialização. 
https://dados.gov.br/dados/conjuntos-dados/restricoes-de-venda-de-medicamentos

## Primeiro obstáculo para conseguir fazer este gráfico foi a quantidade de registros
Somando todas as linhas dos 95 arquivos csv's teríamos quase meio Bilhão de linhas na tabela o que se mostrou inviável dentro dos meus recursos disponíveis.  
Apesar de ser possível inserir  meio Bilhão de linhas em uma tabela MySQL, quando for realizar uma consulta provavelmente não vai concluir a não ser que faça paginação (fazer a consulta em partes menores).
Portanto decidi não analisar todo o Brasil e me restringir a pesquisa ao Estado do Rio de Janeiro.

## Escrevi um post do que fiz no MySQL para conseguir obter resultados desta tabela
https://www.areadetrampo.com.br/quando-sua-tabela-tem-30-milhoes-de-registros-no-mysql/

# Curiosidade
O filme **Império da Dor na NetFlix** mostrou um caso de uma agente federal que percebeu o aumento esponencial de um determinado remédio controlado nos Estados Unidos, o que a levou a uma investigação em campo.

# Objetivo deste projeto
Levantamento científico para estudantes de medicina, para que possam associar o aumento de doenças relacionando o aumento de uso de drogas (lícitas ou não lícitas) à Municípios próximos a hospitais e clínicas.



