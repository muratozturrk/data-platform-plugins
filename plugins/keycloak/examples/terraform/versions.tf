terraform {
  required_version = ">= 1.11" # write-only arguments and ephemeral variables (OpenTofu: check equivalent support)
  required_providers {
    keycloak = {
      source  = "keycloak/keycloak"
      version = "~> 5.10"
    }
  }
  # Remote, encrypted state; access limited to the IAM pipeline.
  backend "s3" {}
}
