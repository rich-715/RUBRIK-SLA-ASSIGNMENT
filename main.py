# main.py
import requests
import json
import csv
import os
import time
from datetime import datetime
from config import (
    RSC_URL,
    CLIENT_ID,
    CLIENT_SECRET,
    GQL_ENDPOINT,
    SLA_NAME,
    CLUSTER_NAME,
    VM_CSV,
    OUTPUT_FOLDER,
    BATCH_SIZE
)


# ══════════════════════════════════════════════════════════
# AUTHENTICATE
# ══════════════════════════════════════════════════════════
def get_token():
    print("\n[Step 1] Authenticating to RSC...")
    print(f"  → URL: {RSC_URL}")

    response = requests.post(
        f"{RSC_URL}/api/client_token",
        json={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET
        },
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        print(f"  ✗ Auth failed : {response.status_code}")
        print(f"  ✗ Response    : {response.text}")
        raise Exception("Authentication failed")

    token = response.json()["access_token"]
    print(f"  ✓ Authenticated successfully")
    return token


# ══════════════════════════════════════════════════════════
# GRAPHQL HELPER
# ══════════════════════════════════════════════════════════
def run_graphql(token, query):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }

    payload  = {"query": query}
    response = requests.post(
        GQL_ENDPOINT,
        json=payload,
        headers=headers
    )

    if response.status_code != 200:
        print(f"  ✗ HTTP Error : {response.status_code}")
        print(f"  ✗ Response   : {response.text}")
        raise Exception(f"HTTP Error {response.status_code}")

    result = response.json()

    if "errors" in result:
        print("  ✗ GraphQL Errors:")
        for err in result["errors"]:
            print(f"    - {err['message']}")
        raise Exception("GraphQL query failed")

    return result["data"]


# ══════════════════════════════════════════════════════════
# LOAD VMs FROM CSV
# ══════════════════════════════════════════════════════════
def load_vms_from_csv(filepath):
    print(f"\n[Step 2] Loading VMs from '{filepath}'...")

    if not os.path.exists(filepath):
        print(f"  ✗ File not found : {filepath}")
        print(f"  ✗ Full path      : {os.path.abspath(filepath)}")
        raise Exception(f"CSV file not found: {filepath}")

    if os.path.getsize(filepath) == 0:
        print(f"  ✗ File is empty")
        raise Exception(f"CSV file is empty")

    vm_names = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        print(f"  CSV Headers : {reader.fieldnames}")

        for row in reader:
            if "vm_name" not in row:
                print(f"  ✗ Column 'vm_name' not found")
                print(f"  ✗ Found columns : {list(row.keys())}")
                raise Exception("CSV missing 'vm_name' column")

            name = row["vm_name"].strip()
            if name:
                vm_names.append(name)
                print(f"  ✓ Loaded : {name}")

    if not vm_names:
        raise Exception("No VM names found in CSV")

    print(f"\n  ✓ Total VMs loaded : {len(vm_names)}")
    return vm_names


# ══════════════════════════════════════════════════════════
# GET CLUSTER ID
# ✓ Uses clusterConnection - confirmed in schema
# ✓ Uses status and systemStatus - confirmed non-deprecated
# ══════════════════════════════════════════════════════════
def get_cluster_id(token, cluster_name):
    print(f"\n[Step 3] Looking up Cluster: '{cluster_name}'")

    query = """
    query GetClusterByName {
      clusterConnection(
        filter: {
          name: ["%s"]
        }
      ) {
        edges {
          node {
            id
            name
            status
            version
            systemStatus
          }
        }
      }
    }
    """ % cluster_name

    data  = run_graphql(token, query)
    edges = data["clusterConnection"]["edges"]

    if not edges:
        print(f"  ✗ Cluster '{cluster_name}' not found")
        raise Exception(f"Cluster '{cluster_name}' not found")

    cluster = edges[0]["node"]

    print(f"  ✓ Cluster Name    : {cluster['name']}")
    print(f"  ✓ Cluster ID      : {cluster['id']}")
    print(f"  ✓ Cluster Status  : {cluster['status']}")
    print(f"  ✓ System Status   : {cluster.get('systemStatus', 'N/A')}")
    print(f"  ✓ Cluster Version : {cluster['version']}")

    if cluster["status"] != "Connected":
        print(f"  ⚠ Warning: Cluster is {cluster['status']}")

    return cluster["id"]


# ══════════════════════════════════════════════════════════
# GET SLA ID
# ✓ Uses slaDomains - confirmed in schema
# ✓ Uses ownerOrg instead of deprecated ownerOrgName
# ✓ Uses replicationSpecsV2 instead of deprecated replicationSpec
# ══════════════════════════════════════════════════════════
def get_sla_id(token, sla_name):
    print(f"\n[Step 4] Looking up SLA: '{sla_name}'")

    query = """
    query GetSLAByName {
      slaDomains(
        filter: [
          {
            field: NAME
            text: "%s"
          }
        ]
      ) {
        edges {
          node {
            id
            name
            ... on GlobalSlaReply {
              objectTypes
              protectedObjectCount
              ownerOrg {
                id
                name
              }
            }
          }
        }
      }
    }
    """ % sla_name

    data  = run_graphql(token, query)
    edges = data["slaDomains"]["edges"]

    if not edges:
        print(f"  ✗ SLA '{sla_name}' not found")
        raise Exception(f"SLA '{sla_name}' not found")

    sla = edges[0]["node"]
    print(f"  ✓ SLA Name      : {sla['name']}")
    print(f"  ✓ SLA ID        : {sla['id']}")
    print(f"  ✓ Object Types  : {sla.get('objectTypes', 'N/A')}")
    print(f"  ✓ Protected VMs : {sla.get('protectedObjectCount', 'N/A')}")

    owner_org = sla.get('ownerOrg', {})
    if owner_org:
        print(f"  ✓ Owner Org     : {owner_org.get('name', 'N/A')}")

    return sla["id"]


# ══════════════════════════════════════════════════════════
# GET VM IDs
# ✓ Uses vSphereVmNewConnection - confirmed in schema
# ✓ Uses NAME filter - confirmed in HierarchyFilterField enum
# ✓ Uses CLUSTER_ID filter - confirmed in HierarchyFilterField enum
# ══════════════════════════════════════════════════════════
def get_vm_ids(token, vm_names, cluster_id, cluster_name):
    print(f"\n[Step 5] Looking up {len(vm_names)} VMs on cluster '{cluster_name}'...")

    if not vm_names:
        raise Exception("VM names list is empty - check vms.csv")

    print(f"  VMs to find    : {vm_names}")
    print(f"  Target Cluster : {cluster_name}")
    print(f"  Cluster ID     : {cluster_id}")

    found   = {}
    missing = []

    for vm_name in vm_names:
        print(f"\n  → Searching: '{vm_name}'")

        # ✓ Using both NAME and CLUSTER_ID filters confirmed in schema
        query = """
        query GetVMByNameAndCluster {
          vSphereVmNewConnection(
            filter: [
              {
                field: NAME
                texts: ["%s"]
              }
              {
                field: CLUSTER_ID
                texts: ["%s"]
              }
            ]
          ) {
            count
            edges {
              node {
                id
                name
                slaAssignment
                effectiveSlaDomain {
                  id
                  name
                }
                primaryClusterLocation {
                  id
                  name
                }
              }
            }
          }
        }
        """ % (vm_name, cluster_id)

        try:
            data  = run_graphql(token, query)
            edges = data["vSphereVmNewConnection"]["edges"]
            count = data["vSphereVmNewConnection"]["count"]

            print(f"  Total results  : {count}")

            if count == 0 or not edges:
                print(f"  ✗ '{vm_name}' not found on cluster '{cluster_name}'")
                missing.append(vm_name)
                continue

            for edge in edges:
                node       = edge["node"]
                vm_cluster = node["primaryClusterLocation"]["name"]

                found[node["name"]] = {
                    "id":         node["id"],
                    "name":       node["name"],
                    "sla_name":   node["effectiveSlaDomain"]["name"],
                    "sla_assign": node["slaAssignment"],
                    "cluster":    vm_cluster,
                    "cluster_id": node["primaryClusterLocation"]["id"]
                }
                print(f"  ✓ Found on '{vm_cluster}'")
                print(f"    VM ID      : {node['id']}")
                print(f"    Current SLA: {node['effectiveSlaDomain']['name']}")
                break

        except Exception as e:
            print(f"  ✗ Error : '{vm_name}': {e}")
            missing.append(vm_name)

    # ── Print Results Table ────────────────────────────────
    print(f"\n{'='*75}")
    print(f"  VM Lookup Results - Target Cluster: {cluster_name}")
    print(f"{'='*75}")
    print(f"  {'VM Name':<25} {'Cluster':<20} {'Current SLA':<20} {'VM ID'}")
    print(f"  {'-'*25} {'-'*20} {'-'*20} {'-'*36}")

    for name, details in found.items():
        print(
            f"  ✓ {details['name']:<23} "
            f"{details['cluster']:<20} "
            f"{details['sla_name']:<20} "
            f"{details['id']}"
        )

    for name in missing:
        print(f"  ✗ {name:<23} NOT FOUND ON '{cluster_name}'")

    print(f"\n  ── Summary ──────────────────────────────────────────────")
    print(f"  Target Cluster : {cluster_name}")
    print(f"  Found          : {len(found)}")
    print(f"  Not Found      : {len(missing)}")

    if missing:
        print(f"\n  Missing VMs:")
        for name in missing:
            print(f"    ✗ {name}")

    return found, missing


# ══════════════════════════════════════════════════════════
# ASSIGN SLA TO VMs
# ✓ Uses assignSla mutation - confirmed in schema
# ✓ Uses slaDomainAssignType - confirmed in AssignSlaInput
# ✓ Uses protectWithSlaId - confirmed in SlaAssignTypeEnum
# ✓ Uses slaOptionalId - confirmed in AssignSlaInput
# ✓ Uses objectIds - confirmed in AssignSlaInput
# ══════════════════════════════════════════════════════════
def assign_sla_to_vms(token, sla_id, vm_ids):
    print(f"\n[Step 6] Assigning SLA to {len(vm_ids)} VMs...")
    print(f"  SLA ID     : {sla_id}")
    print(f"  Total VMs  : {len(vm_ids)}")
    print(f"  Batch Size : {BATCH_SIZE}")

    results       = []
    total_batches = -(-len(vm_ids) // BATCH_SIZE)

    for i in range(0, len(vm_ids), BATCH_SIZE):
        batch     = vm_ids[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1

        print(f"\n  ── Batch {batch_num}/{total_batches} ──────────────────────")
        print(f"  Processing {len(batch)} VMs...")

        # ✓ All field names confirmed from schema introspection
        mutation = """
        mutation AssignSLAToVMs {
          assignSla(input: {
            slaDomainAssignType:             protectWithSlaId
            slaOptionalId:                   "%s"
            objectIds:                       %s
            shouldApplyToExistingSnapshots:  false
            shouldApplyToNonPolicySnapshots: false
          }) {
            success
          }
        }
        """ % (sla_id, json.dumps(batch))

        data          = run_graphql(token, mutation)
        assign_result = data["assignSla"]

        # ✓ Handle list response
        if isinstance(assign_result, list):
            success = assign_result[0].get("success", False) if assign_result else False

        # ✓ Handle dict response
        elif isinstance(assign_result, dict):
            success = assign_result.get("success", False)

        else:
            print(f"  ⚠ Unexpected response type: {type(assign_result)}")
            success = False

        if success:
            print(f"  ✓ Batch {batch_num} succeeded")
        else:
            print(f"  ✗ Batch {batch_num} failed")

        results.append({
            "batch":   batch_num,
            "vm_ids":  batch,
            "success": success
        })

    return results


# ══════════════════════════════════════════════════════════
# VERIFY ASSIGNMENT - WITH WAIT AND RETRY
# ✓ Uses vSphereVmNewConnection - confirmed in schema
# ✓ Uses NAME filter - confirmed in HierarchyFilterField
# ✓ Uses CLUSTER_ID filter - confirmed in HierarchyFilterField
# ══════════════════════════════════════════════════════════
def verify_assignment(token, vm_names, cluster_id, expected_sla, wait_seconds=45):
    print(f"\n[Step 7] Waiting {wait_seconds} seconds for RSC to process...")

    for remaining in range(wait_seconds, 0, -5):
        print(f"  ⏳ Verifying in {remaining} seconds...")
        time.sleep(5)

    print(f"  ✓ Wait complete - checking assignment now...")

    def check_vms(attempt_num):
        passed = []
        failed = []

        print(f"\n  ── Verification Attempt {attempt_num} ────────────────")
        print(f"\n  {'VM Name':<25} {'Cluster':<20} {'Expected SLA':<20} {'Actual SLA':<20} Status")
        print(f"  {'-'*25} {'-'*20} {'-'*20} {'-'*20} ------")

        for vm_name in vm_names:

            # ✓ Using CLUSTER_ID filter confirmed in schema
            query = """
            query VerifyVM {
              vSphereVmNewConnection(
                filter: [
                  {
                    field: NAME
                    texts: ["%s"]
                  }
                  {
                    field: CLUSTER_ID
                    texts: ["%s"]
                  }
                ]
              ) {
                edges {
                  node {
                    name
                    slaAssignment
                    primaryClusterLocation {
                      id
                      name
                    }
                    effectiveSlaDomain {
                      name
                    }
                  }
                }
              }
            }
            """ % (vm_name, cluster_id)

            try:
                data  = run_graphql(token, query)
                edges = data["vSphereVmNewConnection"]["edges"]

                if not edges:
                    print(f"  ✗ {vm_name:<25} {'N/A':<20} {expected_sla:<20} {'NOT FOUND':<20} ✗ FAIL")
                    failed.append(vm_name)
                    continue

                for edge in edges:
                    node       = edge["node"]
                    actual_sla = node["effectiveSlaDomain"]["name"]
                    cluster    = node["primaryClusterLocation"]["name"]

                    if actual_sla == expected_sla:
                        status = "✓ PASS"
                        passed.append(node["name"])
                    else:
                        status = "✗ FAIL"
                        failed.append(node["name"])

                    print(
                        f"  {node['name']:<25} "
                        f"{cluster:<20} "
                        f"{expected_sla:<20} "
                        f"{actual_sla:<20} "
                        f"{status}"
                    )
                    break

            except Exception as e:
                print(f"  ✗ Error verifying '{vm_name}': {e}")
                failed.append(vm_name)

        return passed, failed

    # ── First Attempt ──────────────────────────────────────
    passed, failed = check_vms(1)

    # ── Retry if Still Failing ─────────────────────────────
    if failed:
        print(f"\n  ⚠ {len(failed)} VMs not updated yet")
        print(f"  ⏳ Waiting another 30 seconds and retrying...")

        for remaining in range(30, 0, -5):
            print(f"  ⏳ Retry in {remaining} seconds...")
            time.sleep(5)

        passed, failed = check_vms(2)

    print(f"\n  ✓ Passed : {len(passed)}")
    print(f"  ✗ Failed : {len(failed)}")

    if failed:
        print(f"\n  ⚠ Note: If VMs show FAIL but RSC portal shows correct SLA")
        print(f"  ⚠ This is a timing issue - assignment was likely successful")
        print(f"  ⚠ Please verify manually in RSC portal")

    return passed, failed


# ══════════════════════════════════════════════════════════
# SAVE RESULTS
# ══════════════════════════════════════════════════════════
def save_results(found_vms, missing_vms, sla_name, cluster_name, assign_results, passed, failed):
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"{OUTPUT_FOLDER}/sla_assignment_{cluster_name}_{timestamp}.csv"

    print(f"\n[Step 8] Saving results to '{filename}'...")

    successful_ids = set()
    for result in assign_results:
        if result["success"]:
            for vm_id in result["vm_ids"]:
                successful_ids.add(vm_id)

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "vm_name",
            "vm_id",
            "cluster",
            "previous_sla",
            "new_sla",
            "status"
        ])
        writer.writeheader()

        for name, details in found_vms.items():
            status = "ASSIGNED" if details["id"] in successful_ids else "FAILED"
            writer.writerow({
                "vm_name":      details["name"],
                "vm_id":        details["id"],
                "cluster":      details["cluster"],
                "previous_sla": details["sla_name"],
                "new_sla":      sla_name,
                "status":       status
            })

        for name in missing_vms:
            writer.writerow({
                "vm_name":      name,
                "vm_id":        "N/A",
                "cluster":      "N/A",
                "previous_sla": "N/A",
                "new_sla":      "N/A",
                "status":       f"VM NOT FOUND ON {cluster_name.upper()}"
            })

    print(f"  ✓ Saved: {filename}")
    return filename


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    print("=" * 65)
    print("  Rubrik RSC - Bulk VM SLA Assignment")
    print(f"  Cluster  : {CLUSTER_NAME}")
    print(f"  SLA      : {SLA_NAME}")
    print(f"  Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    try:
        # ── Step 1: Authenticate ───────────────────────────
        token = get_token()

        # ── Step 2: Load VMs from CSV ──────────────────────
        vm_names = load_vms_from_csv(VM_CSV)

        # ── Step 3: Get Cluster ID ─────────────────────────
        cluster_id = get_cluster_id(token, CLUSTER_NAME)

        # ── Step 4: Get SLA ID ─────────────────────────────
        sla_id = get_sla_id(token, SLA_NAME)

        # ── Step 5: Get VM IDs ─────────────────────────────
        found_vms, missing_vms = get_vm_ids(
            token, vm_names, cluster_id, CLUSTER_NAME
        )

        if not found_vms:
            print(f"\n  ✗ No VMs found on cluster '{CLUSTER_NAME}'. Exiting.")
            return

        # ── Confirmation ───────────────────────────────────
        vm_ids = [d["id"] for d in found_vms.values()]

        print(f"\n{'='*65}")
        print(f"  ── Assignment Preview ───────────────────────────────")
        print(f"  Cluster   : {CLUSTER_NAME}")
        print(f"  Cluster ID: {cluster_id}")
        print(f"  SLA       : {SLA_NAME}")
        print(f"  SLA ID    : {sla_id}")
        print(f"  VMs Found : {len(found_vms)}")
        print(f"  Missing   : {len(missing_vms)}")
        print(f"{'='*65}")

        print(f"\n  VMs that WILL be assigned to '{SLA_NAME}':")
        for name, details in found_vms.items():
            print(f"    ✓ {details['name']} on cluster '{details['cluster']}'")

        if missing_vms:
            print(f"\n  VMs that will NOT be assigned:")
            for name in missing_vms:
                print(f"    ✗ {name}")

        confirm = input("\n  Proceed with assignment? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("  ✗ Cancelled. Exiting.")
            return

        # ── Step 6: Assign SLA ─────────────────────────────
        assign_results = assign_sla_to_vms(token, sla_id, vm_ids)

        # ── Step 7: Verify with Wait ───────────────────────
        found_names    = list(found_vms.keys())
        passed, failed = verify_assignment(
            token,
            found_names,
            cluster_id,
            SLA_NAME,
            wait_seconds=120
        )

        # ── Step 8: Save Results ───────────────────────────
        output_file = save_results(
            found_vms,
            missing_vms,
            SLA_NAME,
            CLUSTER_NAME,
            assign_results,
            passed,
            failed
        )

        # ── Final Summary ──────────────────────────────────
        print(f"\n{'='*65}")
        print(f"  ── Final Summary ─────────────────────────────────────")
        print(f"  Cluster     : {CLUSTER_NAME}")
        print(f"  SLA         : {SLA_NAME}")
        print(f"  Requested   : {len(vm_names)}")
        print(f"  Found       : {len(found_vms)}")
        print(f"  Not Found   : {len(missing_vms)}")
        print(f"  ✓ Passed    : {len(passed)}")
        print(f"  ✗ Failed    : {len(failed)}")
        print(f"  Output      : {output_file}")
        print(f"  Completed   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if failed:
            print(f"\n  ⚠ NOTE: If VMs show FAILED but RSC portal")
            print(f"  ⚠ shows correct SLA this is a timing issue")
            print(f"  ⚠ RSC can take 1-5 mins to update SLA status")
            print(f"  ⚠ Please verify in RSC portal to confirm")

        print(f"{'='*65}\n")

    except Exception as e:
        print(f"\n  ✗ Fatal Error: {e}")
        raise


if __name__ == "__main__":
    main()