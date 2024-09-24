from enum import Enum, auto
import os
import struct

from pager import Pager


class ExecuteResult(Enum):
    SUCCESS = auto()
    TABLE_FULL = auto()


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

    def __init__(self, filename):
        self.pager = Pager(filename)
        self.num_rows = self.pager.file_length // self.ROW_SIZE

    def row_memory_data(self, row_num) -> memoryview:
        page_num = row_num // self.ROWS_PER_PAGE
        page = self.pager.get_page(page_num)
        row_offset = row_num % self.ROWS_PER_PAGE
        byte_offset = row_offset * self.ROW_SIZE
        return memoryview(page)[byte_offset: byte_offset + self.ROW_SIZE]


    def insert_row(self, row: Row) -> ExecuteResult:
        if self.num_rows >= self.TABLE_MAX_ROWS:
            return ExecuteResult.TABLE_FULL
        row_data = row.serialize()
        self.row_memory_data(self.num_rows)[:] = row_data
        self.num_rows += 1
        return ExecuteResult.SUCCESS

    def select_all(self):
        for i in range(self.num_rows):
            row_data = self.row_memory_data(i)
            row = Row.deserialize(row_data)
            print(row)

        return ExecuteResult.SUCCESS


def db_close(table: Table):
    pager = table.pager
    num_full_pages = table.num_rows // table.ROWS_PER_PAGE

    for i in range(num_full_pages):
        if pager.pages[i] is None:
            continue

        pager.flush(i, Pager.PAGE_SIZE)
        pager.pages[i] = None

    num_additional_rows = table.num_rows % table.ROWS_PER_PAGE
    if num_additional_rows > 0:
        page_num = num_full_pages
        if pager.pages[page_num] is not None:
            pager.flush(page_num, num_additional_rows * table.ROW_SIZE)
            pager.pages[page_num] = None

    os.close(pager.file_descriptor)

    for i in range(Pager.TABLE_MAX_PAGES):
        pager.pages[i] = None