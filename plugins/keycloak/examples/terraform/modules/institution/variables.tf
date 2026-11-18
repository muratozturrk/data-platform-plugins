variable "realm_id" { type = string }
variable "realm_name" { type = string }
variable "tenant_id" {
  type = string
  validation {
    condition     = can(regex("^t-[a-z0-9-]{2,30}$", var.tenant_id))
    error_message = "tenant_id must look like t-acme (naming-conventions)."
  }
}
variable "institution" {
  type = object({
    name        = string
    domains     = list(string)
    roles       = list(string)
    admin_group = string
    idp = object({
      type         = string
      alias        = string
      issuer       = string
      metadata_url = string
      client_id    = string
      trust_email  = bool
      group_map    = map(string)
    })
  })
}
