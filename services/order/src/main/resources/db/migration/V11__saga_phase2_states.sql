-- Phase 2 saga states: fraud check (ADR 0005) and, later, fulfillment.
ALTER TABLE saga_instances DROP CONSTRAINT saga_instances_state_check;
ALTER TABLE saga_instances ADD CONSTRAINT saga_instances_state_check CHECK (state IN (
    'PENDING', 'INVENTORY_RESERVED', 'PAYMENT_AUTHORIZED', 'FRAUD_APPROVED', 'PAYMENT_CAPTURED', 'FULFILLING',
    'COMPLETED', 'COMPENSATING', 'CANCELLED', 'NEEDS_ATTENTION'));
