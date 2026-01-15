#!/usr/bin/env python3
"""
Script to read and explore database tables.
Usage: python scripts/db_reader.py
"""

import os
import sys
from typing import Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Please install psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def get_connection_string() -> str:
    """
    Get database connection string from environment variable or command line argument.
    Set DATABASE_URL environment variable with your connection string.
    Example: export DATABASE_URL="postgresql://user:password@host:port/database"
    Or pass as first command line argument.
    """
    # First try command line argument
    if len(sys.argv) > 1:
        return sys.argv[1]
    
    # Then try environment variable
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        print("Error: DATABASE_URL environment variable not set and no connection string provided.")
        print("\nPlease set it using one of these methods:")
        print("1. Export it: export DATABASE_URL='postgresql://user:pass@host:port/db'")
        print("2. Pass it inline: DATABASE_URL='...' python scripts/db_reader.py")
        print("3. Pass as argument: python scripts/db_reader.py 'postgresql://user:pass@host:port/db'")
        sys.exit(1)
    return conn_str


def list_tables(conn) -> list:
    """List all tables in the database."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name;
        """)
        return cursor.fetchall()


def get_table_schema(conn, table_name: str, schema: str = "public") -> list:
    """Get column information for a specific table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema, table_name))
        return cursor.fetchall()


def get_table_row_count(conn, table_name: str, schema: str = "public") -> int:
    """Get the number of rows in a table."""
    with conn.cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}";')
        return cursor.fetchone()[0]


def get_table_sample(conn, table_name: str, schema: str = "public", limit: int = 10):
    """Get a sample of rows from a table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(f'SELECT * FROM "{schema}"."{table_name}" LIMIT %s;', (limit,))
        return cursor.fetchall()


def main():
    """Main function to explore the database."""
    conn_str = get_connection_string()
    
    try:
        conn = psycopg2.connect(conn_str)
        print("✓ Successfully connected to database\n")
        
        # List all tables
        print("=" * 60)
        print("DATABASE TABLES")
        print("=" * 60)
        tables = list_tables(conn)
        if not tables:
            print("No tables found in the database.")
            return
        
        for table in tables:
            schema = table['table_schema']
            name = table['table_name']
            row_count = get_table_row_count(conn, name, schema)
            print(f"\n{schema}.{name} ({row_count:,} rows)")
            
            # Get schema
            columns = get_table_schema(conn, name, schema)
            print("  Columns:")
            for col in columns:
                col_type = col['data_type']
                if col['character_maximum_length']:
                    col_type += f"({col['character_maximum_length']})"
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                print(f"    - {col['column_name']}: {col_type} ({nullable})")
            
            # Get sample data
            if row_count > 0:
                print(f"\n  Sample data (first {min(5, row_count)} rows):")
                sample = get_table_sample(conn, name, schema, limit=5)
                if sample:
                    print(f"    Columns: {', '.join(sample[0].keys())}")
                    for i, row in enumerate(sample, 1):
                        row_str = ", ".join([f"{k}={v}" for k, v in list(row.items())[:5]])
                        if len(row) > 5:
                            row_str += "..."
                        print(f"    Row {i}: {row_str}")
        
        conn.close()
        print("\n" + "=" * 60)
        print("Done!")
        
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
