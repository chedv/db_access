from sqlalchemy.engine.base import Connection
from sqlalchemy import Table, select, and_

from typing import List, Tuple


class BaseDAO:
    def __init__(self, table: Table, connection: Connection):
        self.table = table
        self.connection = connection

    def get_rows(self, filter_columns: List[Tuple[str, str]], output_columns: List[str]):
        filters = [self.table.columns[name] == value for name, value in filter_columns]
        outputs = [self.table.columns[name] for name in output_columns]

        select_stmt = select(outputs).where(and_(*filters))
        return self.connection.execute(select_stmt).fetchall()

    def get_join_stmt(self, join_table: Table, left_id: str, right_id: str):
        return self.table.join(join_table, self.table.columns[left_id] == join_table.columns[right_id])


class CinemaDAO(BaseDAO):
    def get_cinema_id(self, cinema_name: str) -> int:
        rows = self.get_rows(filter_columns=[('cinema_name', cinema_name)], output_columns=['id'])
        return rows[0][0] if rows else None


class CinemaSessionDAO(BaseDAO):
    def get_cinema_sessions(self, cinema_id: int, output_columns: List[str], to_join: Tuple[Table, str, str]):
        join_table, left_id, right_id = to_join
        join_stmt = self.get_join_stmt(join_table, left_id, right_id)

        select_stmt = select(output_columns).select_from(join_stmt).where(self.table.columns.cinema_id == cinema_id)

        return self.connection.execute(select_stmt).fetchall()
