# Optimized Keycloak image for HKM: build-time options baked in, providers and themes included.
# Pin the exact version; keep it equal to keycloak_version in Terraform.
FROM quay.io/keycloak/keycloak:26.8.2 AS builder
ENV KC_DB=postgres \
    KC_HEALTH_ENABLED=true \
    KC_METRICS_ENABLED=true
# Custom providers (event listener, mappers) and the HKM login theme, if any.
COPY providers/ /opt/keycloak/providers/
COPY themes/ /opt/keycloak/themes/
RUN /opt/keycloak/bin/kc.sh build

FROM quay.io/keycloak/keycloak:26.8.2
COPY --from=builder /opt/keycloak/ /opt/keycloak/
COPY conf/blocklist.txt /opt/keycloak/data/password-blacklists/blocklist.txt
ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]
