from enum import Enum, auto
import os
from pathlib import Path
import struct

from btree import BTree, BTreeNode
from constants import FILENAME
from pager import Pager
from row import Row


class ExecuteResult(Enum):
    SUCCESS = auto()
    TABLE_FULL = auto()

class Cursor:
    def __init__(self, table, row_num, end_of_table):
        self.table = table
        self.row_num = row_num
        self.end_of_table = end_of_table


class Table:
    ROWS_PER_PAGE = Pager.PAGE_SIZE // Row.ROW_SIZE
    TABLE_MAX_ROWS = ROWS_PER_PAGE * Pager.TABLE_MAX_PAGES

    def __init__(self, filename=FILENAME):
        self.pager = Pager(filename)
        self.num_rows = 0
        self.btree = BTree(3)
        self.load_btree()

    def table_start(self):
        cursor = Cursor(self, 0, False)
        return cursor

    def table_end(self):
        cursor = Cursor(self, self.num_rows, True)
        return cursor

    def cursor_value(self, cursor: Cursor):
        row = self.btree.search(cursor.row_num)
        if row is not None:
            return row
        return None

    def cursor_advance(self, cursor: Cursor):
        cursor.row_num += 1
        if cursor.row_num >= self.TABLE_MAX_ROWS:
            cursor.end_of_table = True

    def insert_row(self, row: Row) -> ExecuteResult:
        if self.num_rows >= self.TABLE_MAX_ROWS:
            return ExecuteResult.TABLE_FULL
        
        self.btree.insert(row.id, row)
        self.num_rows += 1
        return ExecuteResult.SUCCESS

    def select_all(self):
        cursor = self.table_start()
        while not cursor.end_of_table:
            row = self.cursor_value(cursor)
            if row:
                print(row)
            self.cursor_advance(cursor)

        return ExecuteResult.SUCCESS
    
    def save_btree(self):
        BTreeNode.serialize_and_store(self.btree.root, self.pager, is_root=True)
        print(f'persisted BTree to {FILENAME}')

    def load_btree(self):
        if self.pager.get_page(0) is not None:
            root = self.btree.root.deserialize(self.pager, 0)
            self.btree.root = root


def db_close(table: Table):
    table.save_btree()
    os.close(table.pager.file_descriptor)
