# Databricks notebook source
# Leitura da tabela salva como Delta
df_metrics = spark.read.format("delta").load("/Volumes/workspace/default/ifood/user_metrics_abtest_delta")

# Visualizar esquema
df_metrics.printSchema()

# Converte para pandas para análises estatísticas e gráficos
df_pd = df_metrics.toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC **Análise de Retenção**

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# Retenção em 7 dias
plt.figure(figsize=(8, 5))
ax = sns.barplot(data=retencao_d7, x="is_target", y="retention_d7_pct", palette="Blues")

plt.title("Retenção em 7 dias (%)")
plt.ylabel("% usuários retidos")
plt.xlabel("Grupo")
plt.ylim(0, 30)

for i, bar in enumerate(ax.patches):
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2,  
        height + 0.5,                      
        f"{height:.1f}%",                  
        ha='center', va='bottom', fontsize=10
    )

plt.show()


# COMMAND ----------

from scipy.stats import ttest_ind

# Teste estatístico
group_target = df_pd[df_pd["is_target"] == "target"]["retention_d7"]
group_control = df_pd[df_pd["is_target"] == "control"]["retention_d7"]
t_stat, p_val = ttest_ind(group_target, group_control)
print(f"Teste t para retenção 7d — p-valor: {p_val:.4f}")


# COMMAND ----------

# MAGIC %md
# MAGIC **Análise de Viabilidade Financeira**

# COMMAND ----------

from pyspark.sql.functions import col, when, avg, round as spark_round

# KPIs de interesse
kpis = ["retention_d3", "retention_d7", "avg_ticket", "total_value"]

# Calcular médias por grupo
df_kpis_avg = df_metrics.groupBy("is_target").agg(
    *[spark_round(avg(kpi), 2).alias(f"avg_{kpi}") for kpi in kpis]
)

df_kpis_avg.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

pdf_kpis_avg = df_kpis_avg.toPandas()

# COMMAND ----------

# Extrair ticket médio do grupo tratamento
ticket_tratamento = pdf_kpis_avg.loc[pdf_kpis_avg['is_target'] == 'target', 'avg_avg_ticket'].values[0]

# Extrair gasto médio por grupo
gasto_tratamento = pdf_kpis_avg.loc[pdf_kpis_avg['is_target'] == 'target', 'avg_total_value'].values[0]
gasto_controle = pdf_kpis_avg.loc[pdf_kpis_avg['is_target'] == 'control', 'avg_total_value'].values[0]

ganho_incremental = gasto_tratamento - gasto_controle

# Criar DataFrame para calculo do ROI
custos_percentuais = [0.05, 0.10, 0.15]
custos = [ticket_tratamento * p for p in custos_percentuais]
rois = [ganho_incremental / c if c != 0 else None for c in custos]

df_roi = pd.DataFrame({
    'Custo (%)': ['5%', '10%', '15%'],
    'Custo (R$)': custos,
    'ROI': rois
})


# COMMAND ----------

# Ticket Médio por Grupo
plt.figure(figsize=(8,5))
ax = sns.barplot(data=pdf_kpis_avg, x='is_target', y='avg_avg_ticket', palette='Blues')
plt.title('Ticket Médio por Grupo')
plt.ylabel('Ticket Médio (R$)')
plt.xlabel('Grupo')

for bar in ax.patches:
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width()/2,
        height + 0.5,
        f"R$ {height:.2f}",
        ha='center', va='bottom', fontsize=10
    )

plt.show()


# COMMAND ----------

# Gasto Médio Total por Grupo
plt.figure(figsize=(8,5))
ax = sns.barplot(data=pdf_kpis_avg, x='is_target', y='avg_total_value', palette='Greens')
plt.title('Gasto Médio Total por Grupo')
plt.ylabel('Gasto Médio (R$)')
plt.xlabel('Grupo')

for bar in ax.patches:
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width()/2,
        height + 0.5,
        f"R$ {height:.2f}",
        ha='center', va='bottom', fontsize=10
    )

plt.show()


# COMMAND ----------

# Roi Estimado para Diferentes Cenários de Custo
plt.figure(figsize=(8,5))
ax = sns.barplot(data=df_roi, x='Custo (%)', y='ROI', palette='Purples')
plt.axhline(1, color='red', linestyle='--', label='ROI = 1 (Ponto de equilíbrio)')
plt.title('ROI Estimado para Diferentes Cenários de Custo')
plt.ylabel('ROI (Ganho / Custo)')
plt.xlabel('Custo do Incentivo')
plt.legend()

for bar in ax.patches:
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width()/2,
        height + 0.05,
        f"{height:.2f}",
        ha='center', va='bottom', fontsize=10
    )

plt.show()
