import os
import sqlite3
import pandas as pd
import streamlit as st

DBS = {
    "YeetCode": os.environ.get("YEETCODE_DB_PATH", "/app/data/yeetcode.db"),
    "Companies": os.environ.get("COMPANIES_DB_PATH", "/app/data/companies.db"),
}

st.set_page_config(page_title="YeetCode DB Viewer", layout="wide")

st.sidebar.title("DB Viewer")
db_name = st.sidebar.selectbox("Database", list(DBS.keys()))
DB_PATH = DBS[db_name]
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


# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.title("YeetCode DB")
tables = get_tables()
page = st.sidebar.radio("Page", ["Dashboard"] + tables)

# ── Dashboard ──────────────────────────────────────────────────────────────────
if page == "Dashboard":
    st.title("Dashboard")
    conn = get_conn()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Users", pd.read_sql_query("SELECT COUNT(*) FROM users", conn).iloc[0, 0])
    col2.metric("Daily Problems", pd.read_sql_query("SELECT COUNT(*) FROM daily_problems", conn).iloc[0, 0])
    col3.metric("Bounties", pd.read_sql_query("SELECT COUNT(*) FROM bounties", conn).iloc[0, 0])
    col4.metric("Bounty Progress Entries", pd.read_sql_query("SELECT COUNT(*) FROM bounty_progress", conn).iloc[0, 0])

    st.subheader("Top 10 Users by XP")
    top_users = pd.read_sql_query(
        "SELECT username, xp FROM users ORDER BY xp DESC LIMIT 10", conn
    ).set_index("username")
    st.bar_chart(top_users)

    st.subheader("Difficulty Breakdown (Daily Problems)")
    diff = pd.read_sql_query(
        "SELECT difficulty, COUNT(*) as count FROM daily_problems GROUP BY difficulty", conn
    ).set_index("difficulty")
    st.bar_chart(diff)

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

        df_edit = df.astype(object).where(pd.notna(df), None)
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
            return v

        if st.button("Save Changes", key=f"save_{table}"):
            if not pk_cols:
                st.warning("No primary key defined — cannot save changes automatically. Use Raw SQL.")
            elif edited.shape != df_edit.shape:
                st.error(
                    f"Row count changed ({df_edit.shape[0]} → {edited.shape[0]}). "
                    "Use the Add Row tab to insert; this tab only updates existing rows."
                )
            else:
                changes = []
                for idx in edited.index:
                    row_new = edited.loc[idx]
                    row_old = df_edit.loc[idx]
                    diffs = {
                        c: _norm(row_new[c])
                        for c in edited.columns
                        if _norm(row_new[c]) != _norm(row_old[c])
                    }
                    if diffs:
                        changes.append((diffs, {c: _norm(row_new[c]) for c in pk_cols}))

                if not changes:
                    st.info("No changes detected.")
                else:
                    conn = get_conn()
                    executed = []
                    try:
                        for diffs, pk_vals in changes:
                            set_cols = [c for c in diffs.keys() if c not in pk_cols]
                            if not set_cols:
                                continue
                            set_clause = ", ".join(f"{c} = ?" for c in set_cols)
                            where_clause = " AND ".join(f"{c} = ?" for c in pk_cols)
                            values = [diffs[c] for c in set_cols]
                            pk_values = [pk_vals[c] for c in pk_cols]
                            sql_stmt = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
                            conn.execute(sql_stmt, values + pk_values)
                            executed.append((sql_stmt, values + pk_values))
                        conn.commit()
                        st.success(f"Updated {len(executed)} row(s).")
                        with st.expander("Statements executed"):
                            for stmt, params in executed:
                                st.code(f"{stmt}\n-- params: {params}", language="sql")
                    except Exception as e:
                        st.error(f"Error: {e}")
                    finally:
                        conn.close()

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
