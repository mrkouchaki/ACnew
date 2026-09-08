








# =====================================================================
# RECOVER ONLY TDR STAGING
#
# Keeps the existing 4.6M-row handoff_v1 untouched.
# Reads tdrrec30.csv and stages ONLY TDR into a temporary table.
# The existing empty tdr_v1 is replaced only AFTER the new staging
# completes successfully.
# =====================================================================

import time
import sqlite3
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------------------
# 1. Safety checks
# ---------------------------------------------------------------------

if not Path(DB_PATH).exists():
    raise FileNotFoundError(DB_PATH)

if not TDR_PATH.exists():
    raise FileNotFoundError(
        f"TDR CSV not found: {TDR_PATH}"
    )


def _table_exists(name):
    return (
        con.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table'
              AND name=?
            """,
            (name,)
        ).fetchone()
        is not None
    )


if not _table_exists("handoff_v1"):
    raise RuntimeError(
        "handoff_v1 is missing. Stop here."
    )


handoff_rows_check = con.execute(
    "SELECT COUNT(*) FROM handoff_v1"
).fetchone()[0]

if handoff_rows_check == 0:
    raise RuntimeError(
        "handoff_v1 is empty. Stop here."
    )


print(
    f"Existing handoff_v1 preserved: "
    f"{handoff_rows_check:,} rows"
)


# ---------------------------------------------------------------------
# 2. Inspect REAL tdrrec30.csv BEFORE spending time staging it
# ---------------------------------------------------------------------

tdr_header = read_csv_header(
    TDR_PATH
)

print(
    f"TDR CSV columns found: {len(tdr_header):,}"
)


# Join identifiers physically present in tdrrec30.csv
tdr_join_columns = [
    c
    for c in JOIN_KEY_COLUMNS
    if c in tdr_header
]


print(
    "TDR candidate join identifiers:",
    tdr_join_columns
)


if not tdr_join_columns:
    raise RuntimeError(
        "STOP: tdrrec30.csv itself contains none of the configured "
        "exact join identifiers. Do not stage it yet."
    )


# ---------------------------------------------------------------------
# 3. Determine only the TDR columns needed by the notebook
# ---------------------------------------------------------------------

tdr_wanted = set(
    c
    for c in JOIN_KEY_COLUMNS
    if c in tdr_header
)


for aliases in TDR_ALIASES.values():
    tdr_wanted.update(
        c
        for c in aliases
        if c in tdr_header
    )


# Useful time/call fields when present
tdr_wanted.update(
    c
    for c in [
        "CALL_BEGIN_TIME_UTC",
        "CALL_DATE_UTC",
        "CALL_ID",
        "CALLID",
    ]
    if c in tdr_header
)


tdr_wanted = sorted(
    tdr_wanted
)


print(
    f"TDR columns to stage: {len(tdr_wanted)}"
)

print(
    tdr_wanted
)


# ---------------------------------------------------------------------
# 4. Read a tiny sample first
# ---------------------------------------------------------------------

sample = pd.read_csv(
    TDR_PATH,
    dtype="string",
    nrows=5,
    usecols=lambda c: canon(c) in set(tdr_wanted),
    low_memory=False,
    encoding_errors="replace",
)


sample.columns = [
    canon(c)
    for c in sample.columns
]


print(
    f"\nSample rows read: {len(sample)}"
)

display(
    sample.head()
)


if sample.empty:
    raise RuntimeError(
        "tdrrec30.csv contains no data rows."
    )


sample_join_columns = [
    c
    for c in JOIN_KEY_COLUMNS
    if c in sample.columns
]


print(
    "Join columns confirmed in sample:",
    sample_join_columns
)


# ---------------------------------------------------------------------
# 5. Stage into a NEW temporary table.
#
# IMPORTANT:
# We DO NOT touch existing tdr_v1 yet.
# ---------------------------------------------------------------------

recovery_table = (
    "tdr_v1_recovery_"
    + str(int(time.time()))
)


print(
    "\nTemporary recovery table:",
    recovery_table
)


tdr_rows_new = 0

start_time = time.time()


for chunk_no, chunk in enumerate(
    pd.read_csv(
        TDR_PATH,
        dtype="string",
        chunksize=CSV_CHUNK_ROWS,
        usecols=lambda c: canon(c) in set(tdr_wanted),
        low_memory=False,
        encoding_errors="replace",
    ),
    start=1,
):

    chunk = prepare_stage_chunk(
        chunk,
        "TDR",
        tdr_rows_new,
    )


    chunk.to_sql(
        recovery_table,
        con,
        if_exists="append",
        index=False,
        chunksize=5_000,
    )


    tdr_rows_new += len(
        chunk
    )


    if (
        chunk_no == 1
        or tdr_rows_new % 500_000 < len(chunk)
    ):
        elapsed_min = (
            time.time()
            - start_time
        ) / 60.0

        print(
            f"Staged TDR rows: "
            f"{tdr_rows_new:,} "
            f"({elapsed_min:.1f} min)"
        )


con.commit()


# ---------------------------------------------------------------------
# 6. Validate recovered table BEFORE replacing anything
# ---------------------------------------------------------------------

if tdr_rows_new == 0:
    raise RuntimeError(
        "Recovery staging produced zero TDR rows. "
        "Existing tables were NOT changed."
    )


recovery_cols = table_columns(
    con,
    recovery_table
)


recovery_norm_keys = [
    c
    for c in recovery_cols
    if c.startswith("NORM__")
]


print(
    f"\nRecovered TDR rows: "
    f"{tdr_rows_new:,}"
)

print(
    f"Recovered columns : "
    f"{len(recovery_cols):,}"
)

print(
    "Normalized join keys:",
    recovery_norm_keys
)


if not recovery_norm_keys:
    raise RuntimeError(
        "Recovered TDR has no normalized join keys. "
        "Existing tdr_v1 was NOT changed."
    )


# ---------------------------------------------------------------------
# 7. Confirm current tdr_v1 is really empty
# ---------------------------------------------------------------------

old_tdr_rows = 0

if _table_exists("tdr_v1"):
    old_tdr_rows = con.execute(
        "SELECT COUNT(*) FROM tdr_v1"
    ).fetchone()[0]


print(
    f"Existing tdr_v1 rows: "
    f"{old_tdr_rows:,}"
)


if old_tdr_rows > 0:
    raise RuntimeError(
        "Existing tdr_v1 is NOT empty, so it was not replaced. "
        "Inspect it before continuing."
    )


# ---------------------------------------------------------------------
# 8. Preserve the old empty table and promote recovered TDR
#
# Nothing is deleted.
# ---------------------------------------------------------------------

if _table_exists("tdr_v1"):

    backup_name = (
        "tdr_v1_empty_backup_"
        + str(int(time.time()))
    )

    con.execute(
        f'ALTER TABLE "tdr_v1" '
        f'RENAME TO "{backup_name}"'
    )

    print(
        "Preserved previous empty table as:",
        backup_name
    )


con.execute(
    f'ALTER TABLE "{recovery_table}" '
    f'RENAME TO "tdr_v1"'
)


con.commit()


# ---------------------------------------------------------------------
# 9. Add indexes to recovered TDR
# ---------------------------------------------------------------------

tdr_cols = table_columns(
    con,
    "tdr_v1"
)


for column in tdr_cols:

    if column.startswith(
        "NORM__"
    ):

        con.execute(
            f"CREATE INDEX IF NOT EXISTS "
            f"{qident('ix_t_' + column)} "
            f"ON tdr_v1({qident(column)})"
        )


con.commit()


# Refresh variables expected by Section 3
tdr_rows = con.execute(
    "SELECT COUNT(*) FROM tdr_v1"
).fetchone()[0]

handoff_cols = table_columns(
    con,
    "handoff_v1"
)

tdr_cols = table_columns(
    con,
    "tdr_v1"
)


# ---------------------------------------------------------------------
# 10. Final verification
# ---------------------------------------------------------------------

stage_summary = pd.DataFrame(
    [
        {
            "SOURCE": "HANDOFF",
            "ROWS": handoff_rows_check,
            "COLUMNS": len(handoff_cols),
            "STATUS": "PRESERVED_EXISTING",
        },
        {
            "SOURCE": "TDR",
            "ROWS": tdr_rows,
            "COLUMNS": len(tdr_cols),
            "STATUS": "RECOVERED_FROM_CSV",
        },
    ]
)


display(
    stage_summary
)


print(
    "\nTDR recovery complete."
)

print(
    "handoff_v1 was NOT rebuilt."
)

print(
    "SQLite file was NOT deleted."
)

print(
    "Now rerun Section 3: "
    "'Select the strongest safe exact join'."
)

































# ================================================================
# REUSE EXISTING ESINET SQLITE STAGING -- DO NOT DELETE OR RESTAGE
# ================================================================

import sqlite3
from pathlib import Path
import pandas as pd

# Never rebuild the expensive staging database
REBUILD_STAGING = False

if not Path(DB_PATH).exists():
    raise FileNotFoundError(
        f"Existing ESInet SQLite not found: {DB_PATH}"
    )

# Open existing DB
con = sqlite3.connect(DB_PATH)

con.execute("PRAGMA journal_mode=WAL")
con.execute("PRAGMA synchronous=NORMAL")
con.execute("PRAGMA temp_store=FILE")


def existing_table(name):
    return con.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table'
          AND name=?
        """,
        (name,)
    ).fetchone() is not None


required = [
    "handoff_v1",
    "tdr_v1",
]

missing = [
    t for t in required
    if not existing_table(t)
]

if missing:
    con.close()
    raise RuntimeError(
        "Existing SQLite is incomplete. Missing table(s): "
        + ", ".join(missing)
        + ". Do NOT delete the DB; inspect which staging step was incomplete."
    )


handoff_rows = con.execute(
    "SELECT COUNT(*) FROM handoff_v1"
).fetchone()[0]

tdr_rows = con.execute(
    "SELECT COUNT(*) FROM tdr_v1"
).fetchone()[0]

handoff_cols = table_columns(
    con,
    "handoff_v1"
)

tdr_cols = table_columns(
    con,
    "tdr_v1"
)


# Ensure indexes exist; this does NOT rebuild data.
for column in handoff_cols:
    if column.startswith("NORM__"):
        con.execute(
            f"CREATE INDEX IF NOT EXISTS "
            f"{qident('ix_h_' + column)} "
            f"ON handoff_v1({qident(column)})"
        )

for column in tdr_cols:
    if column.startswith("NORM__"):
        con.execute(
            f"CREATE INDEX IF NOT EXISTS "
            f"{qident('ix_t_' + column)} "
            f"ON tdr_v1({qident(column)})"
        )

con.execute(
    "CREATE UNIQUE INDEX IF NOT EXISTS "
    "ux_handoff_call_v1 ON handoff_v1(CALL_KEY)"
)

con.commit()


stage_summary = pd.DataFrame([
    {
        "SOURCE": "HANDOFF",
        "ROWS": handoff_rows,
        "COLUMNS": len(handoff_cols),
        "STATUS": "REUSED_EXISTING"
    },
    {
        "SOURCE": "TDR",
        "ROWS": tdr_rows,
        "COLUMNS": len(tdr_cols),
        "STATUS": "REUSED_EXISTING"
    },
])

display(stage_summary)

print("\nExisting SQLite reused successfully.")
print("No files deleted. No CSV restaging performed.")
print("Continue with Section 3: Select the strongest safe exact join.")


Ok now i have it. Now think carefully, these are misrouted calls. Assume i want to make an ml model that can be trained on these calls and next this trained model can predict or detect these failures . So we need to find most corrolated or asdociated features or clumns from those three tables of mgmlc, ims, ccdr that if these situation happens we have some failures. But we shiuld not involve the columns or features that are the concequence of this misrouting. Because on this way model will be distracted on features that can show failures but these are happened because if that misrouting. Give me one cell i can add to the end of my psap_gmlc_routing v5 notebook and it can read psap_routing_definite_misroutes_filtered_by_routed_distance.csv and can get other related features from i think sqllite or other outputs . And give one corrolation or association map of features and the percentage of its impact on problem. We need to plot it also. Then put this option :write the code in order if we looked st results and see some features are not important i can simply add to a list to exclude thise features and rerun. You can use any model, maybe two. Before i used random forest. See whats the best