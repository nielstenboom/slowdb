from enum import Enum, auto
import os
import struct

from constants import FILENAME
from pager import Pager


class ExecuteResult(Enum):
    SUCCESS = auto()
    TABLE_FULL = auto()

class Cursor:
    def __init__(self, table, row_num, end_of_table):
        self.table = table
        self.row_num = row_num
        self.end_of_table = end_of_table

class Row:
    COLUMN_USERNAME_SIZE = 32
    COLUMN_EMAIL_SIZE = 255

    def __init__(self, row_id=0, username="", email=""):
        self.id = row_id
        self.username = username[:self.COLUMN_USERNAME_SIZE]
        self.email = email[:self.COLUMN_EMAIL_SIZE]

    def serialize(self) -> bytes:
        """Pack the row into bytes for storage."""
        return struct.pack(f"I{self.COLUMN_USERNAME_SIZE}s{self.COLUMN_EMAIL_SIZE}s", 
                            self.id, self.username.encode(), self.email.encode())

    @staticmethod
    def deserialize(data: bytes):
        """Unpack bytes back into a Row."""
        id_, username, email = struct.unpack(f"I{Row.COLUMN_USERNAME_SIZE}s{Row.COLUMN_EMAIL_SIZE}s", data)
        return Row(id_, username.decode().strip('\x00'), email.decode().strip('\x00'))
        # return Row(id_, username.decode(), email.decode())

    def __str__(self):
        return f"({self.id}, {self.username}, {self.email})"


class Table:
    ROW_SIZE = struct.calcsize(f"I{Row.COLUMN_USERNAME_SIZE}s{Row.COLUMN_EMAIL_SIZE}s")
    ROWS_PER_PAGE = Pager.PAGE_SIZE // ROW_SIZE
    TABLE_MAX_ROWS = ROWS_PER_PAGE * Pager.TABLE_MAX_PAGES

    def __init__(self, filename=FILENAME):
        self.pager = Pager(filename)
        self.num_rows = 0

    def table_start(self):
        cursor = Cursor(self, 0, self.num_rows == 0)
        return cursor

    def table_end(self):
        cursor = Cursor(self, self.num_rows, True)
        return cursor

    def cursor_value(self, cursor: Cursor):
        row_num = cursor.row_num
        page_num = row_num // self.ROWS_PER_PAGE
        page = self.pager.get_page(page_num)
        row_offset = row_num % self.ROWS_PER_PAGE
        byte_offset = row_offset * self.ROW_SIZE
        return memoryview(page)[byte_offset:byte_offset + self.ROW_SIZE]

    def cursor_advance(self, cursor: Cursor):
        cursor.row_num += 1
        if cursor.row_num >= self.num_rows:
            cursor.end_of_table = True

    def insert_row(self, row: Row) -> ExecuteResult:
        if self.num_rows >= self.TABLE_MAX_ROWS:
            return ExecuteResult.TABLE_FULL
        
        cursor = self.table_end()
        row_data = row.serialize()
        self.cursor_value(cursor)[:] = row_data
        self.num_rows += 1
        return ExecuteResult.SUCCESS

    def select_all(self):
        cursor = self.table_start()
        while not cursor.end_of_table:
            row_data = self.cursor_value(cursor)
            row = Row.deserialize(row_data)
            print(row)
            self.cursor_advance(cursor)
        return ExecuteResult.SUCCESS


def db_close(table: Table):
    os.close(table.pager.file_descriptor)
