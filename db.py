from enum import Enum, auto
import struct


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
        # return Row(id_, username.decode().strip('\x00'), email.decode().strip('\x00'))
        return Row(id_, username.decode(), email.decode())

    def __str__(self):
        return f"({self.id}, {self.username}, {self.email})"


class Table:
    PAGE_SIZE = 4096
    TABLE_MAX_PAGES = 100
    ROW_SIZE = struct.calcsize(f"I{Row.COLUMN_USERNAME_SIZE}s{Row.COLUMN_EMAIL_SIZE}s")
    ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
    TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

    def __init__(self):
        self.num_rows = 0
        self.pages = [None] * self.TABLE_MAX_PAGES

    def row_slot(self, row_num) -> memoryview:
        page_num = row_num // self.ROWS_PER_PAGE
        if self.pages[page_num] is None:
            self.pages[page_num] = bytearray(self.PAGE_SIZE)
        row_offset = row_num % self.ROWS_PER_PAGE
        byte_offset = row_offset * self.ROW_SIZE
        return memoryview(self.pages[page_num])[byte_offset:byte_offset + self.ROW_SIZE]

    def insert_row(self, row: Row) -> ExecuteResult:
        if self.num_rows >= self.TABLE_MAX_ROWS:
            return ExecuteResult.TABLE_FULL
        row_data = row.serialize()
        print(f"{row_data=}")
        self.row_slot(self.num_rows)[:] = row_data
        self.num_rows += 1
        return ExecuteResult.SUCCESS

    def select_all(self):
        for i in range(self.num_rows):
            row_data = self.row_slot(i)
            row = Row.deserialize(row_data)
            print(row)

        return ExecuteResult.SUCCESS
