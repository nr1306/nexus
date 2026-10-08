-- One database per service (CLAUDE.md rule 4). Runs once, on first container start.
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE fraud_db;
CREATE DATABASE fulfillment_db;
