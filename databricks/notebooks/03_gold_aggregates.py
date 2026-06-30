# Databricks notebook source
# MAGIC %pip install -r ../../requirements.txt

# Task: [INS-202] - Agregados de Negócio e KPIs para Power BI (Camada Gold)
from pyspark.sql.functions import col, sum, count, avg, round, when, current_timestamp

# 1. DEFINIÇÃO DOS AMBIENTES NO UNITY CATALOG
CATALOG = "insurance_platform"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

# Garantir que o esquema gold existe antes de gravar
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

print("🚀 A iniciar o processamento da Camada Gold...")

# 2. CARREGAR AS TABELAS DA CAMADA SILVER
df_cust = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.dim_customers")
df_pols = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_policies")
df_clms = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_claims")

# 3. CRIAR FAIXAS ETÁRIAS PARA ANÁLISE DEMOGRÁFICA
df_cust_enriched = df_cust.withColumn(
    "age_group",
    when(col("age") < 25, "18-24 (Jovem)")
    .when((col("age") >= 25) & (col("age") <= 55), "25-55 (Adulto)")
    .otherwise("56+ (Sénior)")
)

# 4. UNIFICAR OS DADOS (Join) PARA GERAR A MATRIZ DE RISCO
# Cruzamos Apólices com Clientes para herdar o Distrito e a Faixa Etária
df_pols_enriched = df_pols.join(df_cust_enriched, "customer_id", "inner")

# 5. CALCULAR AGREGADOS FINANCEIROS POR DISTRITO, TIPO SEGURO E IDADE
# Agregado A: Total de Prémios Recebidos (Faturação)
df_premium_agg = df_pols_enriched.groupBy("district", "policy_type", "age_group") \
    .agg(
        sum("premium_amount").alias("total_premiums"),
        count("policy_id").alias("active_policies_count")
    )

# Agregado B: Total de Sinistros Pagos (Apenas os Aprovados)
df_clms_enriched = df_clms.join(df_pols_enriched, "policy_id", "inner") \
    .filter(col("claim_status") == "Approved")

df_claims_agg = df_clms_enriched.groupBy("district", "policy_type", "age_group") \
    .agg(
        sum("claim_amount").alias("total_claims_paid"),
        count("claim_id").alias("approved_claims_count")
    )

# 6. JUNTAR AS DUAS MÉTRICAS E CALCULAR OS KPIs DE SEGUROS (Loss Ratio)
df_gold_metrics = df_premium_agg.join(df_claims_agg, ["district", "policy_type", "age_group"], "left") \
    .na.fill(0) \
    .withColumn("total_premiums", round(col("total_premiums"), 2)) \
    .withColumn("total_claims_paid", round(col("total_claims_paid"), 2)) \
    .withColumn(
        "loss_ratio_pct", 
        round((col("total_claims_paid") / col("total_premiums")) * 100, 2)
    ) \
    .withColumn("gold_timestamp", current_timestamp())

# 7. GRAVAR A TABELA FINAL OPTIMIZADA PARA O POWER BI
df_gold_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.agg_insurance_monthly_kpis")

print("✅ Tabela 'agg_insurance_monthly_kpis' gravada com sucesso na Gold!")
print("⚡ --- CAMADA GOLD CONCLUÍDA --- ⚡")
