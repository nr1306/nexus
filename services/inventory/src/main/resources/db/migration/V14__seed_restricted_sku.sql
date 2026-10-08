-- In stock, but Fulfillment refuses to ship it (its restricted_skus): exercises the refund path locally.
INSERT INTO stock (sku, available) VALUES ('SKU-HAZMAT-01', 1000);
