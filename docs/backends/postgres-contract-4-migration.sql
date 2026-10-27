-- Migration from edax table contract 3 to 4: the command outbox gains `after_event`, and every outbox
-- `position` is filled by a sequence with cache 1.
-- Adapt schema/table names to your deployment. Both changes are ones the previous release ignores: run
-- this first, then roll out the new release. Run through your migration tool; edax never executes this file.
--
-- A relay reads an outbox in `position` order. A sequence that caches a block of values per session can
-- hand an aggregate's later write an earlier position, so the relay would send it first.
-- A session keeps the values it already cached until it disconnects: restart every pool that writes these
-- tables after the migration.
BEGIN;
ALTER TABLE public.edax_command_outbox ADD COLUMN IF NOT EXISTS after_event text;
ALTER TABLE public.edax_event_outbox ALTER COLUMN position SET CACHE 1;
ALTER TABLE public.edax_command_outbox ALTER COLUMN position SET CACHE 1;
ALTER TABLE public.edax_saga_outbox ALTER COLUMN position SET CACHE 1;
COMMIT;
-- A `position` filled by a serial or a sequence default rather than an identity: ALTER SEQUENCE <it> CACHE 1,
-- and ALTER SEQUENCE <it> OWNED BY <table>.position if it is not owned yet, so the check can find it.
-- Commands enqueued before the migration carry no `after_event`, so they never wait behind an event.
