import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = "yeetcode.db"

st.set_page_config(page_title="YeetCode DB Viewer", layout="wide")


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

        edited = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            key=f"editor_{table}",
        )

        col_save, col_del = st.columns([1, 4])
        with col_save:
            if st.button("Save Changes", key=f"save_{table}"):
                if pk_cols:
                    conn = get_conn()
                    try:
                        changed = edited.compare(df, result_names=("new", "orig"))
                        if changed.empty:
                            st.info("No changes detected.")
                        else:
                            changed_idx = changed.index.unique()
                            updated = 0
                            for idx in changed_idx:
                                row = edited.loc[idx]
                                set_clause = ", ".join(
                                    [f"{c} = ?" for c in edited.columns if c not in pk_cols]
                                )
                                values = [row[c] for c in edited.columns if c not in pk_cols]
                                where_clause = " AND ".join([f"{c} = ?" for c in pk_cols])
                                pk_values = [row[c] for c in pk_cols]
                                conn.execute(
                                    f"UPDATE {table} SET {set_clause} WHERE {where_clause}",
                                    values + pk_values,
                                )
                                updated += 1
                            conn.commit()
                            st.success(f"Updated {updated} row(s).")
                    except Exception as e:
                        st.error(f"Error: {e}")
                    finally:
                        conn.close()
                else:
                    st.warning("No primary key defined — cannot save changes automatically. Use Raw SQL.")

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
