import os
import sqlite3
import tempfile
import pandas as pd
import streamlit as st

st.set_page_config(page_title="YeetCode DB Viewer", layout="wide")

# Where uploaded DBs get persisted across reruns (a Streamlit rerun re-executes
# the script top-to-bottom, so we keep uploads on disk and remember the path
# in session_state).
UPLOAD_DIR = os.path.join(tempfile.gettempdir(), "db-viewer-uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


def discover_dbs():
    """Find candidate .db files in commonly-mounted locations.

    Returns a list of (label, absolute_path) — labels are de-duplicated.
    Searches: env-var presets, /app, /app/data, /data, the repo root if
    mounted at /workspace, and the directory of this script.
    """
    candidates = []

    env_presets = [
        ("YeetCode", os.environ.get("YEETCODE_DB_PATH")),
        ("Companies", os.environ.get("COMPANIES_DB_PATH")),
        ("Yeetcode Preview", os.environ.get("YEETCODE_PREVIEW_DB_PATH")),
    ]
    for label, p in env_presets:
        if p and os.path.isfile(p):
            candidates.append((label, os.path.abspath(p)))

    here = os.path.dirname(os.path.abspath(__file__))
    search_dirs = [
        here,
        "/app",
        "/app/data",
        "/data",
        "/workspace",
        "/workspace/db-viewer",
        "/workspace/yeetcode-api",
        os.path.join(here, "..", "yeetcode-api"),
    ]
    seen = {p for _, p in candidates}
    for d in search_dirs:
        if not d or not os.path.isdir(d):
            continue
        try:
            entries = os.listdir(d)
        except OSError:
            continue
        for name in entries:
            if not name.endswith(".db"):
                continue
            full = os.path.abspath(os.path.join(d, name))
            if full in seen:
                continue
            try:
                if os.path.getsize(full) == 0:
                    continue  # skip obviously-empty placeholders
            except OSError:
                continue
            seen.add(full)
            candidates.append((name, full))
    return candidates


# ── Sidebar: pick a database ──────────────────────────────────────────────────
st.sidebar.title("DB Viewer")

mode = st.sidebar.radio(
    "Source",
    ["Discovered", "Custom path", "Upload"],
    key="db_source",
    help="Discovered: .db files found on disk.  "
    "Custom path: type any absolute path the container can read.  "
    "Upload: send a .db from your machine.",
)

DB_PATH = None
db_name = None

if mode == "Discovered":
    found = discover_dbs()
    if not found:
        st.sidebar.warning(
            "No .db files found. Use Upload, or mount your DB into the container."
        )
    else:
        labels = [f"{lbl} — {p}" for lbl, p in found]
        idx = st.sidebar.selectbox(
            "Database",
            range(len(labels)),
            format_func=lambda i: labels[i],
            key="discovered_idx",
        )
        db_name, DB_PATH = found[idx]

elif mode == "Custom path":
    DB_PATH = st.sidebar.text_input(
        "Absolute path to .db file",
        value=st.session_state.get("custom_db_path", ""),
        key="custom_db_path",
        placeholder="/app/data/companies.db",
    ).strip() or None
    if DB_PATH:
        db_name = os.path.basename(DB_PATH)
        if not os.path.isfile(DB_PATH):
            st.sidebar.error("File not found at that path.")

elif mode == "Upload":
    uploaded = st.sidebar.file_uploader(
        "Upload a .db file",
        type=["db", "sqlite", "sqlite3"],
        key="db_uploader",
    )
    if uploaded is not None:
        target = os.path.join(UPLOAD_DIR, uploaded.name)
        with open(target, "wb") as f:
            f.write(uploaded.getbuffer())
        st.session_state["uploaded_db_path"] = target
        st.session_state["uploaded_db_name"] = uploaded.name
    if "uploaded_db_path" in st.session_state:
        DB_PATH = st.session_state["uploaded_db_path"]
        db_name = st.session_state["uploaded_db_name"]
        st.sidebar.success(f"Using uploaded: {db_name}")

if not DB_PATH or not os.path.isfile(DB_PATH):
    st.title("YeetCode DB Viewer")
    st.info(
        "Pick a database in the sidebar to get started. "
        "The bundled image doesn't ship with your data — use **Upload** to send "
        "a `.db` from your machine, or mount one in and use **Discovered** / "
        "**Custom path**."
    )
    st.stop()

st.sidebar.caption(DB_PATH)


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def get_tables():
    conn = get_conn()
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )
    conn.close()
    return tables["name"].tolist()


def get_table_info(table):
    conn = get_conn()
    info = pd.read_sql_query(f"PRAGMA table_info({table})", conn)
    conn.close()
    return info


def load_table(table, search=""):
    conn = get_conn()
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if search:
        mask = df.apply(
            lambda col: col.astype(str).str.contains(search, case=False, na=False)
        ).any(axis=1)
        df = df[mask]
    return df


def get_pk_cols(table_info):
    return table_info[table_info["pk"] > 0]["name"].tolist()


# ── Sidebar: pages ────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
tables = get_tables()
page = st.sidebar.radio("Page", ["Dashboard"] + tables)

# ── Dashboard ──────────────────────────────────────────────────────────────────
if page == "Dashboard":
    st.title(f"Dashboard — {db_name}")
    conn = get_conn()
    table_set = set(tables)

    # Row-count metric per table, four per row.
    if tables:
        st.subheader("Row counts")
        for i in range(0, len(tables), 4):
            row = tables[i : i + 4]
            cols = st.columns(len(row))
            for col, t in zip(cols, row):
                try:
                    n = pd.read_sql_query(f"SELECT COUNT(*) FROM {t}", conn).iloc[0, 0]
                    col.metric(t, f"{n:,}")
                except Exception as e:
                    col.metric(t, "—")
                    col.caption(f"err: {e}")
    else:
        st.info("No tables in this database.")

    # YeetCode-style charts (only when the relevant tables exist).
    if {"users"}.issubset(table_set):
        st.subheader("Top 10 Users by XP")
        try:
            top_users = pd.read_sql_query(
                "SELECT username, xp FROM users ORDER BY xp DESC LIMIT 10", conn
            ).set_index("username")
            st.bar_chart(top_users)
        except Exception as e:
            st.caption(f"(skipped: {e})")

    if {"daily_problems"}.issubset(table_set):
        st.subheader("Difficulty Breakdown (Daily Problems)")
        try:
            diff = pd.read_sql_query(
                "SELECT difficulty, COUNT(*) as count FROM daily_problems GROUP BY difficulty",
                conn,
            ).set_index("difficulty")
            st.bar_chart(diff)
        except Exception as e:
            st.caption(f"(skipped: {e})")

    # Companies-DB charts.
    if {"company_problems"}.issubset(table_set):
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top 15 Companies by Problem Count")
            top_co = pd.read_sql_query(
                "SELECT company_id, COUNT(*) AS problems "
                "FROM company_problems GROUP BY company_id "
                "ORDER BY problems DESC LIMIT 15",
                conn,
            ).set_index("company_id")
            st.bar_chart(top_co)
        with c2:
            st.subheader("Difficulty Breakdown")
            diff = pd.read_sql_query(
                "SELECT COALESCE(difficulty, '(null)') AS difficulty, COUNT(*) AS count "
                "FROM company_problems GROUP BY difficulty ORDER BY count DESC",
                conn,
            ).set_index("difficulty")
            st.bar_chart(diff)

        st.subheader("Top 20 Most-Asked Problems")
        popular = pd.read_sql_query(
            "SELECT problem_id, title, difficulty, COUNT(*) AS companies "
            "FROM company_problems GROUP BY problem_id, title, difficulty "
            "ORDER BY companies DESC, problem_id ASC LIMIT 20",
            conn,
        )
        st.dataframe(popular, use_container_width=True)

    conn.close()

# ── Table Pages ────────────────────────────────────────────────────────────────
else:
    table = page
    st.title(f"Table: `{table}`")
    table_info = get_table_info(table)
    pk_cols = get_pk_cols(table_info)

    tab_view, tab_add, tab_sql = st.tabs(["View / Edit", "Add Row", "Raw SQL"])

    # ── View / Edit ────────────────────────────────────────────────────────────
    with tab_view:
        search = st.text_input("Search (filters all columns)", key=f"search_{table}")
        df = load_table(table, search)
        st.caption(f"{len(df)} rows")

        df_edit = df.copy()
        st.caption("Tip: clear a cell to set it to NULL (where the column allows it).")
        edited = st.data_editor(
            df_edit,
            use_container_width=True,
            key=f"editor_{table}",
        )

        def _norm(v):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except (TypeError, ValueError):
                pass
            if isinstance(v, str) and v == "":
                return None
            return v

        def _col_py_type(sqlite_type):
            t = (sqlite_type or "").upper()
            if "INT" in t or "BOOL" in t:
                return int
            if any(x in t for x in ("REAL", "FLOA", "DOUB", "NUM")):
                return float
            return str

        col_py_types = {r["name"]: _col_py_type(r["type"]) for _, r in table_info.iterrows()}

        def _coerce(new_v, old_v, col):
            """Coerce new_v to the column's target Python type.
            Returns (value, error_or_None). Target is the type of old_v when available,
            else the column's schema-derived type."""
            if new_v is None:
                return None, None
            if old_v is not None and not isinstance(old_v, str):
                target = type(old_v)
            else:
                target = col_py_types.get(col, str)
            if target is bool:
                target = int
            if isinstance(new_v, target) and not (target is int and isinstance(new_v, bool)):
                return new_v, None
            try:
                if target is int:
                    if isinstance(new_v, float):
                        if not new_v.is_integer():
                            raise ValueError(f"{new_v!r} is not an integer")
                        return int(new_v), None
                    if isinstance(new_v, str):
                        s = new_v.strip()
                        try:
                            return int(s), None
                        except ValueError:
                            f = float(s)
                            if not f.is_integer():
                                raise ValueError(f"{s!r} is not an integer")
                            return int(f), None
                    return int(new_v), None
                if target is float:
                    return float(new_v), None
                return str(new_v), None
            except (ValueError, TypeError) as e:
                return new_v, f"cannot convert {new_v!r} to {target.__name__} ({e})"

        preview_key = f"preview_{table}"
        result_key = f"result_{table}"

        col_preview, col_confirm, col_cancel = st.columns([1, 1, 4])

        with col_preview:
            if st.button("Preview Changes", key=f"prev_btn_{table}"):
                if not pk_cols:
                    st.session_state[preview_key] = {"error": "No primary key defined."}
                elif edited.shape != df_edit.shape:
                    st.session_state[preview_key] = {
                        "error": f"Row count changed ({df_edit.shape[0]} → {edited.shape[0]}). "
                                 "Use the Add Row tab to insert."
                    }
                else:
                    changes = []
                    errors = []
                    for idx in edited.index:
                        row_new = edited.loc[idx]
                        row_old = df_edit.loc[idx]
                        pk_vals = {c: _norm(row_new[c]) for c in pk_cols}
                        pk_desc = ", ".join(f"{k}={v!r}" for k, v in pk_vals.items())
                        diffs = {}
                        for c in edited.columns:
                            old_v = _norm(row_old[c])
                            raw_new = _norm(row_new[c])
                            new_v, err = _coerce(raw_new, old_v, c)
                            if err:
                                errors.append(f"Row `{pk_desc}`, col `{c}`: {err}")
                                continue
                            if new_v != old_v:
                                diffs[c] = (old_v, new_v)
                        if diffs:
                            changes.append({"pk": pk_vals, "diffs": diffs})
                    st.session_state[preview_key] = {"changes": changes, "errors": errors}

        preview = st.session_state.get(preview_key)
        if preview:
            show_confirm = False
            if "error" in preview:
                st.error(preview["error"])
            else:
                changes = preview.get("changes", [])
                errors = preview.get("errors", [])

                if errors:
                    st.error(
                        f"{len(errors)} type-coercion error(s) — fix the cell(s) in the editor, then Preview again. "
                        "Confirm is disabled while errors exist."
                    )
                    with st.expander(f"Show {len(errors)} error(s)", expanded=True):
                        for e in errors:
                            st.markdown(f"- {e}")

                if not changes and not errors:
                    st.info("No changes detected.")
                elif changes:
                    st.warning(f"{len(changes)} row(s) will be updated. Review below, then Confirm.")
                    with st.expander(f"Show {len(changes)} pending change(s)", expanded=True):
                        for ch in changes:
                            pk_str = ", ".join(f"{k}={v!r}" for k, v in ch["pk"].items())
                            st.markdown(f"**Row** `{pk_str}`")
                            for col, (old_v, new_v) in ch["diffs"].items():
                                st.markdown(f"- `{col}`: `{old_v!r}` → `{new_v!r}`")
                    show_confirm = not errors

            if show_confirm:
                with col_confirm:
                    if st.button("Confirm & Apply", key=f"confirm_{table}", type="primary"):
                        conn = get_conn()
                        executed = []
                        try:
                            for ch in preview["changes"]:
                                set_cols = [c for c in ch["diffs"] if c not in pk_cols]
                                if not set_cols:
                                    continue
                                set_clause = ", ".join(f"{c} = ?" for c in set_cols)
                                where_clause = " AND ".join(f"{c} = ?" for c in pk_cols)
                                values = [ch["diffs"][c][1] for c in set_cols]
                                pk_values = [ch["pk"][c] for c in pk_cols]
                                sql_stmt = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
                                conn.execute(sql_stmt, values + pk_values)
                                executed.append((sql_stmt, values + pk_values))
                            conn.commit()
                            st.session_state[result_key] = {"executed": executed}
                        except Exception as e:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            st.session_state[result_key] = {"error": str(e)}
                        finally:
                            conn.close()
                        st.session_state.pop(preview_key, None)
                        st.rerun()

            with col_cancel:
                if st.button("Cancel", key=f"cancel_{table}"):
                    st.session_state.pop(preview_key, None)
                    st.rerun()

        result = st.session_state.get(result_key)
        if result:
            if "error" in result:
                st.error(f"Error: {result['error']}")
            else:
                executed = result["executed"]
                st.success(f"Updated {len(executed)} row(s).")
                with st.expander("Statements executed"):
                    for stmt, params in executed:
                        st.code(f"{stmt}\n-- params: {params}", language="sql")
            if st.button("Dismiss", key=f"dismiss_{table}"):
                st.session_state.pop(result_key, None)
                st.rerun()

    # ── Add Row ────────────────────────────────────────────────────────────────
    with tab_add:
        st.subheader(f"Insert into `{table}`")
        with st.form(key=f"add_{table}"):
            inputs = {}
            for _, col_info in table_info.iterrows():
                col_name = col_info["name"]
                default = col_info["dflt_value"] or ""
                inputs[col_name] = st.text_input(
                    f"{col_name} ({'PK' if col_info['pk'] else col_info['type']})",
                    value=str(default).strip("'") if default else "",
                )
            submitted = st.form_submit_button("Insert Row")
            if submitted:
                conn = get_conn()
                try:
                    cols = list(inputs.keys())
                    vals = [inputs[c] if inputs[c] != "" else None for c in cols]
                    placeholders = ", ".join(["?" for _ in cols])
                    conn.execute(
                        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
                        vals,
                    )
                    conn.commit()
                    st.success("Row inserted.")
                except Exception as e:
                    st.error(f"Error: {e}")
                finally:
                    conn.close()

    # ── Raw SQL ────────────────────────────────────────────────────────────────
    with tab_sql:
        st.subheader("Run SQL")
        sql = st.text_area(
            "SQL",
            value=f"SELECT * FROM {table} LIMIT 50",
            height=120,
            key=f"sql_{table}",
        )
        if st.button("Run", key=f"run_{table}"):
            conn = get_conn()
            try:
                stripped = sql.strip().upper()
                if stripped.startswith("SELECT") or stripped.startswith("PRAGMA"):
                    result = pd.read_sql_query(sql, conn)
                    st.dataframe(result, use_container_width=True)
                else:
                    conn.execute(sql)
                    conn.commit()
                    st.success("Query executed successfully.")
            except Exception as e:
                st.error(f"Error: {e}")
            finally:
                conn.close()
