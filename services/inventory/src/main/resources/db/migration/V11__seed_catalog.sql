-- Simulated catalog for local runs. Benchmarks reseed stock via bench/scenarios.
INSERT INTO stock (sku, available) VALUES
    ('SKU-0001', 1000),
    ('SKU-0002', 1000),
    ('SKU-0003', 1000),
    ('SKU-0004', 500),
    ('SKU-0005', 500),
    ('SKU-0006', 100),
    ('SKU-0007', 100),
    ('SKU-0008', 50),
    ('SKU-0009', 10),
    ('SKU-0010', 0);
