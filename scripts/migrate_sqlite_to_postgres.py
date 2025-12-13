#!/usr/bin/env python3
import os
import sys
import sqlite3
import psycopg2

def main(sqlite_path: str):
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL not set. Example: postgresql://user:pass@host:5432/db")
        sys.exit(1)

    if not os.path.exists(sqlite_path):
        print(f"SQLite file not found: {sqlite_path}")
        sys.exit(1)

    sq = sqlite3.connect(sqlite_path)
    pg = psycopg2.connect(db_url)
    sq.row_factory = sqlite3.Row

    sc = sq.cursor()
    pc = pg.cursor()

    # Ensure tables exist on Postgres (basic schema compatible with server.py)
    # Introspect columns from SQLite and create compatible table on Postgres
    sc.execute("PRAGMA table_info(matches)")
    pragma = sc.fetchall()  # cid, name, type, notnull, dflt_value, pk
    cols = [row[1] for row in pragma]

    # Build CREATE TABLE for Postgres with matching columns
    type_map = {
        'TEXT': 'TEXT',
        'INTEGER': 'INTEGER'
    }
    columns_sql_parts = []
    pk_cols = []
    for _, name, coltype, notnull, dflt_value, pk in pragma:
        pg_type = type_map.get(coltype.upper(), 'TEXT')
        col_sql = f"{name} {pg_type}"
        if notnull:
            col_sql += " NOT NULL"
        if dflt_value is not None:
            col_sql += f" DEFAULT {dflt_value}"
        columns_sql_parts.append(col_sql)
        if pk:
            pk_cols.append(name)

    if pk_cols:
        columns_sql_parts.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

    create_sql = f"CREATE TABLE IF NOT EXISTS matches ({', '.join(columns_sql_parts)});"
    pc.execute(create_sql)

    sc.execute("SELECT * FROM matches")
    rows = sc.fetchall()

    if not rows:
        print("No rows found in SQLite matches.")
    else:
        print(f"Migrating {len(rows)} rows...")
        for row in rows:
            data = dict(row)
            keys = list(data.keys())
            values = [data[k] for k in keys]
            placeholders = ", ".join(["%s"] * len(values))
            cols_sql = ", ".join(keys)
            # Upsert based on primary key columns (assumes matchid is part of PK)
            conflict_target = 'matchid' if 'matchid' in keys else ', '.join(pk_cols) if pk_cols else ''
            if conflict_target:
                updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k not in pk_cols])
                sql = (
                    f"INSERT INTO matches ({cols_sql}) VALUES ({placeholders}) "
                    f"ON CONFLICT ({conflict_target}) DO UPDATE SET {updates}"
                )
            else:
                sql = f"INSERT INTO matches ({cols_sql}) VALUES ({placeholders})"
            pc.execute(sql, values)
        pg.commit()
        print("Migration complete.")

    sc.close(); pc.close(); sq.close(); pg.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: migrate_sqlite_to_postgres.py <sqlite-db-path>")
        sys.exit(1)
    main(sys.argv[1])
