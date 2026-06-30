# Databricks notebook source
# MAGIC %pip install -r ../../requirements.txt

# Task: [INS-201] - Limpeza, Deduplicação e Enriquecimento (Camada Silver)
from pyspark.sql.functions import col, when, to_date, current_timestamp

# 1. DEFINIÇÃO DOS AMBIENTES NO UNITY CATALOG
CATALOG = "insurance_platform"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

# Garantir que o esquema silver existe antes de gravar
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

print("🚀 A iniciar o processamento da Camada Silver...")

# ====================================================
# ENTIDADE 1: CUSTOMERS (Limpeza e Deduplicação)
# ====================================================
df_bronze_cust = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.raw_customers")

df_silver_cust = df_bronze_cust \
    .dropDuplicates(["customer_id"]) \
    .filter((col("age") >= 18) & (col("age") <= 100)) \
    .withColumn("created_at", to_date(col("created_at"))) \
    .withColumn("silver_timestamp", current_timestamp())

df_silver_cust.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.dim_customers")

print("✅ Tabela 'dim_customers' gravada na Silver.")

# ====================================================
# ENTIDADE 2: POLICIES (Limpeza e Padronização)
# ====================================================
df_bronze_pol = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.raw_policies")

df_silver_pol = df_bronze_pol \
    .dropDuplicates(["policy_id"]) \
    .filter(col("premium_amount") > 0) \
    .withColumn("start_date", to_date(col("start_date"))) \
    .withColumn("silver_timestamp", current_timestamp())

df_silver_pol.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.fact_policies")

print("✅ Tabela 'fact_policies' gravada na Silver.")

# ====================================================
# ENTIDADE 3: CLAIMS (Regras Complexas de Negócio)
# ====================================================
df_bronze_clm = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.raw_claims")

df_silver_clm = df_bronze_clm \
    .dropDuplicates(["claim_id"]) \
    .filter(col("claim_amount") >= 0) \
    .withColumn("claim_date", to_date(col("claim_date"))) \
    .withColumn("is_high_value_risk", when(col("claim_amount") > 30000, 1).otherwise(0)) \
    .withColumn("silver_timestamp", current_timestamp())

df_silver_clm.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.fact_claims")

print("✅ Tabela 'fact_claims' gravada na Silver.")
print("⚡ --- CAMADA SILVER PROCESSADA COM SUCESSO --- ⚡")
