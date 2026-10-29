import requests
import json

# Step 1: Get Access Token
token_url = "http://localhost:8181/api/catalog/v1/oauth/tokens"
token_payload = {
    "grant_type": "client_credentials",
    "client_id": "root_user",
    "client_secret": "my_secret_id",
    "scope": "PRINCIPAL_ROLE:ALL"
}

token_response = requests.post(token_url, data=token_payload)
if token_response.status_code == 200:
    access_token = token_response.json().get("access_token")
    print("\n✅ Access Token Retrieved:")
    print("=" * 80)
    print(access_token)
    print("=" * 80)
else:
    print(f"❌ Failed to get access token: {token_response.status_code}")
    print(token_response.text)
    exit()

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Step 2: Create Catalog
catalog_url = "http://localhost:8181/api/management/v1/catalogs"
catalog_payload = {
    "name": "polariscatalog",
    "type": "INTERNAL",
    "properties": {
        "default-base-location": "file:///data"
    },
    "storageConfigInfo": {
        "storageType": "FILE",
        "allowedLocations": ["file:///data"]
    }
}

catalog_response = requests.post(catalog_url, headers=headers, json=catalog_payload)
if catalog_response.status_code == 201:
    print("✅ Catalog created successfully.")
else:
    print("❌ Failed to create catalog:")
    print(catalog_response.status_code, catalog_response.text)
    exit()

# Step 3: Create Catalog Role
role_url = "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles"
role_payload = {
    "catalogRole": {
        "name": "polariscatalogrole"
    }
}

role_response = requests.post(role_url, headers=headers, json=role_payload)
if role_response.status_code == 201:
    print("✅ Catalog role created successfully.")
else:
    print("❌ Failed to create catalog role:")
    print(role_response.status_code, role_response.text)
    exit()

# Step 4: Create Principal
principal_url = "http://localhost:8181/api/management/v1/principals"
principal_payload = {
    "name": "polarisuser",
    "type": "user"
}

principal_response = requests.post(principal_url, headers=headers, json=principal_payload)
if principal_response.status_code == 201:
    data = principal_response.json()
    principal = data.get("principal", {})
    credentials = data.get("credentials", {})

    print("\n✅ Principal Created:")
    print("=" * 80)
    print(f"Name       : {principal.get('name')}")
    print(f"Client ID  : {credentials.get('clientId')}")
    print(f"Client Secret: {credentials.get('clientSecret')}")
    print("=" * 80)
else:
    print("❌ Failed to create principal:")
    print(principal_response.status_code, principal_response.text)

# Step 5: Create Principal Role
principal_role_url = "http://localhost:8181/api/management/v1/principal-roles"
principal_role_payload = {
    "principalRole": {
        "name": "polarisuserrole"
    }
}

principal_role_response = requests.post(principal_role_url, headers=headers, json=principal_role_payload)
if principal_role_response.status_code == 201:
    print("✅ Created principal role.")
else:
    print("❌ Failed to create principal role:")
    print(principal_role_response.status_code, principal_role_response.text)
    exit()

# Step 6: Assign Principal Role to Principal
assign_role_url = "http://localhost:8181/api/management/v1/principals/polarisuser/principal-roles"
assign_role_payload = {
    "principalRole": {
        "name": "polarisuserrole"
    }
}

assign_role_response = requests.put(assign_role_url, headers=headers, json=assign_role_payload)
if assign_role_response.status_code == 201:
    print("✅ Principal role assigned.")
else:
    print("❌ Failed to assign principal role:")
    print(assign_role_response.status_code, assign_role_response.text)
    exit()

# Step 7: Assign Catalog Role to Principal Role
assign_catalog_role_url = "http://localhost:8181/api/management/v1/principal-roles/polarisuserrole/catalog-roles/polariscatalog"
assign_catalog_role_payload = {
    "catalogRole": {
        "name": "polariscatalogrole"
    }
}

assign_catalog_role_response = requests.put(assign_catalog_role_url, headers=headers, json=assign_catalog_role_payload)
if assign_catalog_role_response.status_code == 201:
    print("✅ Catalog role assigned to principal role.")
else:
    print("❌ Failed to assign catalog role:")
    print(assign_catalog_role_response.status_code, assign_catalog_role_response.text)
    exit()

# Step 8: Grant Privileges to Catalog Role
grant_url = "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants"
grant_payload = {
    "grant": {
        "type": "catalog",
        "privilege": "CATALOG_MANAGE_CONTENT"
    }
}

grant_response = requests.put(grant_url, headers=headers, json=grant_payload)
if grant_response.status_code == 201:
    print("✅ Catalog role granted privileges.")
else:
    print("❌ Failed to grant privileges:")
    print(grant_response.status_code, grant_response.text)
    exit()

# Step 9: Print Summary
print("\n🔐 Summary of Credentials")
print("=" * 80)
print(f"Admin Access Token : {access_token}")
print(f"Principal Name     : {principal.get('name')}")
print(f"Client ID          : {credentials.get('clientId')}")
print(f"Client Secret      : {credentials.get('clientSecret')}")
print("=" * 80)
