# Análise de Teste A/B – Case iFood (Data Analyst Senior)

Este repositório contém a solução desenvolvida para o case técnico de Análise de Dados do iFood, com foco em:

1. Definição de indicadores de sucesso da campanha de cupons
2. Análise de viabilidade financeira
4. Direcionamentos e próximos passos sugeridos para o negócio

## Estrutura do Repositório

- 01_transformacao_user_metrics.py: Script de transformação e preparação de dados
- 02_retencao_e_viabilidade_financeira.py: Análise de KPIs, retenção e viabilidade
- Relatorio_Final_Ifood_-_Case_Data_Analyst_Senior.pdf: Documento final em PDF

## Como Executar

A execução do case foi feita no ambiente Databricks, utilizando PySpark e armazenamento em Volumes com Unity Catalog.

### Etapa 1 – Preparação dos dados

O script `01_transformacao_user_metrics.py`:
- Realiza o download dos dados diretamente de uma URL pública
- Lê os arquivos com PySpark
- Processa os dados e salva no formato Delta em um Volume gerenciado pelo Unity Catalog

IMPORTANTE: edite a **linha 15** do arquivo para definir o caminho correto do seu Volume Databricks (ex: `/Volumes/seu_catalogo/seu_schema/nome_do_volume/`).

Exemplo:
volume_path = "/Volumes/meu_catalogo/meu_schema/ifood"

### Etapa 2 – Análise e visualizações

Após rodar o script de transformação, execute o script `02_retencao_e_viabilidade_financeira.py`, que:
- Lê os dados salvos no Volume
- Calcula os KPIs de sucesso e viabilidade
- Gera análises de retenção e viabilidade financeira
- Produz visualizações

## Relatório Final

O relatório completo com as conclusões e recomendações está disponível no arquivo PDF incluído neste repositório:

Relatorio_Final_Ifood_-_Case_Data_Analyst_Senior.pdf

O material é voltado para líderes de negócio e apresenta:
- Interpretação dos KPIs analisados
- Viabilidade da campanha com base em métricas financeiras
- Sugestões de segmentações e próximos passos

## Requisitos

Este projeto foi desenvolvido no ambiente Databricks, com:
- PySpark
- Delta Lake
- Unity Catalog (para persistência dos dados)
