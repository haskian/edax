-- edax Postgres table contract 2: a reference schema. Copy it into your own migration.

CREATE TABLE "public"."edax_events" (
  id text COLLATE "C" NOT NULL,
  stream text COLLATE "C" NOT NULL,
  sequence bigint NOT NULL,
  aggregate_id text NOT NULL,
  tenant text,
  subject text,
  encryption text,
  name text NOT NULL,
  version bigint NOT NULL,
  data bytea NOT NULL,
  correlation_id text NOT NULL,
  causation_id text NOT NULL,
  occurred_at bigint,
  recorded_at bigint NOT NULL,
  extensions text NOT NULL,
  metadata text NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (stream, sequence)
);

CREATE TABLE "public"."edax_snapshots" (
  stream text COLLATE "C" NOT NULL,
  tenant text,
  subject text,
  encryption text,
  type_name text NOT NULL,
  version bigint NOT NULL,
  data bytea NOT NULL,
  PRIMARY KEY (stream)
);

CREATE TABLE "public"."edax_event_outbox" (
  position bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  event_id text COLLATE "C" NOT NULL,
  PRIMARY KEY (position)
);
CREATE INDEX "edax_event_outbox_event_id" ON "public"."edax_event_outbox" (event_id);

CREATE TABLE "public"."edax_command_outbox" (
  position bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  id text COLLATE "C" NOT NULL,
  idempotency_key text,
  aggregate_id text,
  tenant text,
  subject text,
  encryption text,
  name text NOT NULL,
  version bigint NOT NULL,
  data bytea NOT NULL,
  correlation_id text NOT NULL,
  causation_id text NOT NULL,
  issued_at bigint NOT NULL,
  extensions text NOT NULL,
  metadata text NOT NULL,
  PRIMARY KEY (id)
);
CREATE INDEX "edax_command_outbox_position" ON "public"."edax_command_outbox" (position);

CREATE TABLE "public"."edax_projections" (
  projection text COLLATE "C" NOT NULL,
  id text COLLATE "C" NOT NULL,
  version bigint NOT NULL,
  document jsonb NOT NULL,
  PRIMARY KEY (projection, id)
);

CREATE TABLE "public"."edax_ledger" (
  key text COLLATE "C" NOT NULL,
  owner text,
  settled_by text,
  record text NOT NULL,
  claimed_at timestamptz NOT NULL,
  settled_at timestamptz,
  PRIMARY KEY (key)
);

CREATE TABLE "public"."edax_saga_instances" (
  saga text COLLATE "C" NOT NULL,
  id text COLLATE "C" NOT NULL,
  version bigint NOT NULL,
  state text,
  folded text NOT NULL,
  completed boolean NOT NULL,
  completed_at timestamptz,
  PRIMARY KEY (saga, id)
);

CREATE TABLE "public"."edax_saga_outbox" (
  position bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  saga text COLLATE "C" NOT NULL,
  id text COLLATE "C" NOT NULL,
  idempotency_key text,
  aggregate_id text,
  tenant text,
  subject text,
  encryption text,
  name text NOT NULL,
  version bigint NOT NULL,
  data bytea NOT NULL,
  correlation_id text NOT NULL,
  causation_id text NOT NULL,
  issued_at bigint NOT NULL,
  extensions text NOT NULL,
  metadata text NOT NULL,
  PRIMARY KEY (saga, id)
);
CREATE INDEX "edax_saga_outbox_saga_position" ON "public"."edax_saga_outbox" (saga, position);

CREATE TABLE "public"."edax_relay_leases" (
  outbox text COLLATE "C" NOT NULL,
  owner text NOT NULL,
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (outbox)
);
