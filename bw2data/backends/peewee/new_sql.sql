BEGIN;
DROP INDEX IF EXISTS "activitydataset_key";
ALTER TABLE ActivityDataset rename to AD_old;
CREATE TABLE "activitydataset" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "database" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "data" BLOB NOT NULL,
    "location" TEXT,
    "name" TEXT,
    "product" TEXT,
    "type" TEXT
);
INSERT INTO ActivityDataset ("database", "code", "data", "location", "name", "product", "type")
    SELECT substr(key, 0, instr(key, '⊡')),
        substr("key", instr("key", '⊡') + 1),
        "data",
        "location",
        "name",
        "product",
        "type"
    FROM AD_old;
CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("database", "code");
DROP TABLE AD_old;
COMMIT;



BEGIN;
DROP INDEX IF EXISTS "exchangedataset_database";
DROP INDEX IF EXISTS "exchangedataset_input";
DROP INDEX IF EXISTS "exchangedataset_output";
ALTER TABLE ExchangeDataset rename to ED_old;
CREATE TABLE "exchangedataset" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "data" BLOB NOT NULL,
    "input_database" TEXT NOT NULL,
    "input_code" TEXT NOT NULL,
    "output_database" TEXT NOT NULL,
    "output_code" TEXT NOT NULL,
    "type" TEXT NOT NULL
);
INSERT INTO ExchangeDataset ("data", "input_database", "input_code", "output_database", "output_code", "type")
    SELECT "data",
        substr("input", 0, instr("input", '⊡')),
        substr("input", instr("input", '⊡') + 1),
        substr("output", 0, instr("output", '⊡')),
        substr("output", instr("output", '⊡') + 1),
        "type"
    FROM ED_old;
CREATE INDEX "exchangedataset_input" ON "exchangedataset" ("input_database", "input_code");
CREATE INDEX "exchangedataset_output" ON "exchangedataset" ("output_database", "output_code");
DROP TABLE ED_old;
COMMIT;
