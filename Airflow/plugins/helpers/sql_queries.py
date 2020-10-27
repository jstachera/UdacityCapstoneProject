"""
    SQL helper class.
"""


class SqlQueries:
    INSERT_SQL = ("""
        INSERT INTO public.{}
        {}
    """)

    TRUNCATE_SQL = ("""
        TRUNCATE TABLE public.{}
    """)

    def get_insert_statement(self, destination_table, select_statement):
        """ returns insert statement for give destination table and select statement """
        return self.INSERT_SQL.format(destination_table, select_statement)

    def get_truncate_statement(self, destination_table):
        """ returns truncate statement for give destination table name"""
        return self.TRUNCATE_SQL.format(destination_table)
