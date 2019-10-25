from contextlib import closing

from airflow.hooks.postgres_hook import PostgresHook


class FixedPostgresHook(PostgresHook):

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000,
                    replace=False, unique_columns=None):
        """
        Modified way for Postgres to insert a set of tuples into a table,
        a new transaction is created every commit_every rows

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        :param replace: Whether to replace instead of insert
        :type replace: bool
        :param unique_columns: The name of unique columns on which
            conflict might happen
        :type unique_columns: iterable of strings
        """
        _target_fields = list(target_fields)
        if target_fields:
            target_fields = ", ".join(_target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''

        if replace:
            for unique_column in unique_columns:
                _target_fields.remove(unique_column)
            unique_columns = ", ".join(unique_columns)
            unique_columns = "({})".format(unique_columns)

        i = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor()) as cur:
                for i, row in enumerate(rows, 1):
                    lst = []
                    for cell in row:
                        lst.append(self._serialize_cell(cell, conn))
                    values = tuple(lst)
                    placeholders = ["%s", ] * len(values)

                    sql = "INSERT INTO "

                    sql += "{0} {1} VALUES ({2})".format(
                        table,
                        target_fields,
                        ",".join(placeholders))
                    if replace:
                        sql += "  ON CONFLICT ({0}) DO UPDATE SET ".format(
                            unique_columns
                        )
                        sql += ', '.join(
                            ("{key} = excluded.{key}".format(key=key)
                             for key in _target_fields)
                        ) + ';'

                    cur.execute(sql, values)
                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        self.log.info(
                            "Loaded %s into %s rows so far", i, table
                        )

            conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows", i)
