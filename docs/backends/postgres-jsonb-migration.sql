-- Migration from edax table contract 2 to 3: message maps and encryption information become jsonb.
-- Adapt schema/table names to your deployment. Stop every process of the previous release first: its
-- statements bind and read these columns as text, and its table check refuses jsonb.
-- Run through your migration tool; edax never executes this file.
--
-- Contract 2 kept a NUL inside a map; jsonb cannot, so the cast fails on such a row. List them first:
--
--   SELECT 'edax_events' AS t, id FROM public.edax_events
--   WHERE concat(metadata, extensions, encryption) LIKE '%\\u0000%'
--   UNION ALL SELECT 'edax_command_outbox', id FROM public.edax_command_outbox
--   WHERE concat(metadata, extensions, encryption) LIKE '%\\u0000%'
--   UNION ALL SELECT 'edax_saga_outbox', id FROM public.edax_saga_outbox
--   WHERE concat(metadata, extensions, encryption) LIKE '%\\u0000%'
--   UNION ALL SELECT 'edax_snapshots', stream FROM public.edax_snapshots
--   WHERE encryption LIKE '%\\u0000%';
--
-- The pattern also matches an escaped backslash before "u0000", so check each row it lists. The new
-- release refuses a NUL message on decode too, so publishing a listed outbox row helps only if the
-- previous release's consumers handle it: let them, and see the consumer group's lag reach zero, before
-- you stop it. Otherwise edit or remove the row by hand. A listed event must be edited or removed by
-- hand. A sealed one cannot be edited: its maps are part of its authenticated data.
BEGIN;
ALTER TABLE public.edax_events
  ALTER COLUMN encryption TYPE jsonb USING encryption::jsonb,
  ALTER COLUMN extensions TYPE jsonb USING extensions::jsonb,
  ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;
ALTER TABLE public.edax_command_outbox
  ALTER COLUMN encryption TYPE jsonb USING encryption::jsonb,
  ALTER COLUMN extensions TYPE jsonb USING extensions::jsonb,
  ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;
ALTER TABLE public.edax_saga_outbox
  ALTER COLUMN encryption TYPE jsonb USING encryption::jsonb,
  ALTER COLUMN extensions TYPE jsonb USING extensions::jsonb,
  ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;
ALTER TABLE public.edax_snapshots
  ALTER COLUMN encryption TYPE jsonb USING encryption::jsonb;
COMMIT;
-- ALTER COLUMN TYPE rewrites each table under an ACCESS EXCLUSIVE lock. Plan a window for a large one.
