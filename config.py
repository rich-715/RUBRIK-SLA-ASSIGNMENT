# config.py
# ── RSC Credentials ────────────────────────────────────────
RSC_URL       = "https://companyName.my.rubrik.com"
CLIENT_ID     = "Client ID name"
CLIENT_SECRET = "Secret Key"
GQL_ENDPOINT  = f"{RSC_URL}/api/graphql"

# ── Job Settings ───────────────────────────────────────────
SLA_NAME      = "35day"
CLUSTER_NAME  = "Rubrik-Cluster-Name"       # ← Target cluster
VM_CSV        = "vms.csv"                   # ← csv of VM Names 
OUTPUT_FOLDER = "output"                    # ← csv of results of each run created in this folder
BATCH_SIZE    = 50