

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