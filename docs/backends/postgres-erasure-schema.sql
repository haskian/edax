CREATE TABLE "public"."edax_erasure_operations" (
  operation_id text COLLATE "C" PRIMARY KEY,
  scope text COLLATE "C" NOT NULL,
  request_id text COLLATE "C" NOT NULL,
  version bigint NOT NULL CHECK (version > 0),
  record text NOT NULL,
  UNIQUE (scope, request_id)
);
CREATE TABLE "public"."edax_erasure_notifications" (
  operation_id text COLLATE "C" PRIMARY KEY,
  notification text NOT NULL
);
CREATE TABLE "public"."edax_erasure_revocations" (
  scope text COLLATE "C" PRIMARY KEY
);
CREATE INDEX edax_erasure_operations_pending ON "public"."edax_erasure_operations" (operation_id) WHERE record::json->>'stage' <> 'complete';
