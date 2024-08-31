def select_all_from_table(table: str):
    return f"SELECT * FROM information_schema.tables WHERE table_name = '{table}'"
