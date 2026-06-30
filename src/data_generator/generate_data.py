# src/data_generator/generate_data.py
import os
import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker

# Configurar sementes para garantir consistência em testes (Idempotência)
fake = Faker('pt_PT') # Configurado com localização portuguesa para maior realismo
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def generate_enterprise_insurance_data(num_customers=25000):
    print(f"🚀 A iniciar geração em massa de {num_customers} clientes...")

    # ----------------------------------------------------
    # 1. ENTIDADE: CLIENTES (CUSTOMERS)
    # ----------------------------------------------------
    # Distribuição demográfica realista (Jovens, Adultos ativos, Reformados)
    ages = np.random.choice(
        [random.randint(18, 25), random.randint(26, 55), random.randint(56, 82)],
        size=num_customers,
        p=[0.15, 0.60, 0.25]
    )
    
    # Distribuição de Credit Score (Curva normal simulando o mercado)
    credit_scores = np.clip(np.random.normal(670, 90, num_customers).astype(int), 350, 850)
    
    # Data de criação da conta (últimos 4 anos)
    start_date_pool = [datetime.now() - timedelta(days=int(d)) for d in np.random.randint(0, 1460, num_customers)]

    customers = {
        "customer_id": [f"CUST-{100000 + i}" for i in range(num_customers)],
        "name": [fake.name() for _ in range(num_customers)],
        "age": ages,
        "gender": np.random.choice(["M", "F", "O"], size=num_customers, p=[0.48, 0.49, 0.03]),
        "district": np.random.choice(["Lisboa", "Porto", "Braga", "Setúbal", "Coimbra", "Faro"], size=num_customers, p=[0.35, 0.25, 0.15, 0.10, 0.08, 0.07]),
        "credit_score": credit_scores,
        "created_at": start_date_pool
    }
    df_customers = pd.DataFrame(customers)

    # ----------------------------------------------------
    # 2. ENTIDADE: APÓLICES (POLICIES)
    # ----------------------------------------------------
    print("⏳ A gerar apólices correlacionadas...")
    policies_list = []
    policy_counter = 100000

    for idx, cust in df_customers.iterrows():
        # Lógica: Clientes com melhor perfil financeiro compram mais do que 1 seguro
        num_pols = np.random.choice([1, 2, 3], p=[0.65, 0.28, 0.07]) if cust["credit_score"] > 600 else 1
        p_types = np.random.choice(["Auto", "Home", "Health"], size=num_pols, replace=False)

        for p_type in p_types:
            policy_counter += 1
            # Regra de Negócio: Definição de Prémios Base
            base_premium = {"Auto": 450, "Home": 250, "Health": 850}[p_type]
            
            # Penalizações por risco realistas
            if p_type == "Auto" and cust["age"] < 25:
                base_premium *= 1.8  # Jovens pagam quase o dobro no seguro automóvel
            if cust["credit_score"] < 500:
                base_premium *= 1.3  # Pior score financeiro aumenta o risco da apólice
                
            premium = round(float(np.random.normal(base_premium, base_premium * 0.12)), 2)
            
            # A apólice começa ligeiramente após o cliente se registar
            p_start = cust["created_at"] + timedelta(days=random.randint(1, 15))
            
            # Taxa de cancelamento maior em clientes com problemas de crédito
            status_p = [0.88, 0.12] if cust["credit_score"] > 550 else [0.65, 0.35]
            status = np.random.choice(["Active", "Cancelled"], p=status_p)

            policies_list.append({
                "policy_id": f"POL-{policy_counter}",
                "customer_id": cust["customer_id"],
                "policy_type": p_type,
                "premium_amount": premium,
                "start_date": p_start.strftime('%Y-%m-%d'),
                "status": status,
                "updated_at": p_start.strftime('%Y-%m-%d')
            })
            
    df_policies = pd.DataFrame(policies_list)

    # ----------------------------------------------------
    # 3. ENTIDADE: SINISTROS (CLAIMS)
    # ----------------------------------------------------
    print("⏳ A gerar histórico de sinistros e fraudes...")
    claims_list = []
    claim_counter = 100000

    # Mapear idades de volta às apólices para cálculo vetorial do risco
    df_merged_risk = df_policies.merge(df_customers[["customer_id", "age"]], on="customer_id")

    for idx, pol in df_merged_risk.iterrows():
        # Determinar probabilidade de ter um sinistro com base no perfil real de risco
        claim_prob = 0.08  # probabilidade base geral (8%)
        if pol["policy_type"] == "Auto" and pol["age"] < 25:
            claim_prob = 0.22  # Elevado risco em condutores jovens
        elif pol["policy_type"] == "Health" and pol["age"] > 60:
            claim_prob = 0.18  # Maior uso de seguros de saúde em idades avançadas

        if np.random.rand() < claim_prob:
            # Quantidade de acidentes na mesma apólice ao longo dos 4 anos
            num_claims = np.random.choice([1, 2], p=[0.90, 0.10])
            
            for _ in range(num_claims):
                claim_counter += 1
                p_start_dt = datetime.strptime(pol["start_date"], '%Y-%m-%d')
                
                # O acidente ocorre após o início da apólice
                claim_date = p_start_dt + timedelta(days=random.randint(10, 500))
                if claim_date > datetime.now():
                    continue

                # Distribuição Exponencial para os custos (Muitos pequenos, raros catastróficos)
                avg_cost = {"Auto": 2800, "Home": 4500, "Health": 1200}[pol["policy_type"]]
                claim_amount = round(float(np.random.exponential(scale=avg_cost)), 2)
                
                # Garantir limites lógicos mínimos e máximos
                claim_amount = np.clip(claim_amount, 50.00, 85000.00)

                claims_list.append({
                    "claim_id": f"CLM-{claim_counter}",
                    "policy_id": pol["policy_id"],
                    "claim_date": claim_date.strftime('%Y-%m-%d'),
                    "claim_amount": claim_amount,
                    "claim_status": np.random.choice(["Approved", "Denied", "Pending"], p=[0.75, 0.12, 0.13]),
                    "updated_at": claim_date.strftime('%Y-%m-%d')
                })

    df_claims = pd.DataFrame(claims_list)

    # ----------------------------------------------------
    # 4. SALVAGUARDAR FICHEIROS NA LANDING ZONE
    # ----------------------------------------------------
    
if __name__ == "__main__":
    # Caminho profissional isolado no catálogo do projeto
    output_dir = "/Volumes/insurance_platform/bronze/insurance_landing"
    
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    # Executa a geração em massa com as regras reais do negócio de seguros
    cust, pol, clm = generate_enterprise_insurance_data(25000)
    
    # Grava os ficheiros diretamente no novo Volume
    cust.to_csv(f"{output_dir}/customers.csv", index=False)
    pol.to_csv(f"{output_dir}/policies.csv", index=False)
    clm.to_csv(f"{output_dir}/claims.csv", index=False)

    print("\n⚡ --- PROCESSO CONCLUÍDO EM CATÁLOGO ISOLADO --- ⚡")
    print(f"Dados brutos gravados com sucesso no Volume: {output_dir}/")
