-- Polaris does not create databases: one database per realm (used by the {realm} JDBC URL test).
CREATE DATABASE my_realm;
\c my_realm
CREATE SCHEMA polaris_schema;
