-- Additive migration from edax table contract 1 to 2.
-- Adapt schema/table names to your deployment. Readers must deploy before encrypted writers.
-- Run through your migration tool; edax never executes this file.
BEGIN;
ALTER TABLE public.edax_events ADD COLUMN subject text, ADD COLUMN encryption text;
ALTER TABLE public.edax_command_outbox ADD COLUMN subject text, ADD COLUMN encryption text;
ALTER TABLE public.edax_saga_outbox ADD COLUMN subject text, ADD COLUMN encryption text;
ALTER TABLE public.edax_snapshots ADD COLUMN tenant text, ADD COLUMN subject text, ADD COLUMN encryption text;
COMMIT;
-- Install postgres-erasure-schema.sql separately before enabling erasure coordination.
-- Existing rows remain plaintext; this migration does not rewrite, anonymize or erase them.
