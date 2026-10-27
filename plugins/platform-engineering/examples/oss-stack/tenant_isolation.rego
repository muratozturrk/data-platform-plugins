# OPA policy skeleton used by Trino/Airflow/Kafka authorizers (Stackable) and your own API.
# Tenant comes from the Keycloak token (organization claim); data bundle maps tenants to catalogs/schemas.
package platform.authz

import rego.v1

default allow := false

tenant := input.identity.claims.organization

# Catalog/schema belongs to the caller's tenant
allow if {
    input.action.operation in {"SelectFromColumns", "ShowSchemas", "ShowTables"}
    data.tenants[tenant].catalogs[input.action.resource.table.catalogName]
    startswith(input.action.resource.table.schemaName, sprintf("%s_", [tenant]))
}

# Role-based write within tenant
allow if {
    input.action.operation in {"InsertIntoTable", "CreateTable"}
    data.tenants[tenant].catalogs[input.action.resource.table.catalogName]
    "writer" in input.identity.claims.roles
}

# Column masking for classified columns
column_mask := m if {
    some col in data.classified[tenant][input.action.resource.table.schemaName][input.action.resource.table.tableName]
    col.name == input.action.resource.column.columnName
    not "pii-reader" in input.identity.claims.roles
    m := col.mask
}
