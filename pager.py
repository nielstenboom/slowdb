import os
import sys

from constants import FILENAME

class Pager:
    PAGE_SIZE = 4096
    TABLE_MAX_PAGES = 100

    def __init__(self, filename=FILENAME):
        self.file_descriptor = os.open(filename, os.O_RDWR | os.O_CREAT)
        self.file_length = os.lseek(self.file_descriptor, 0, os.SEEK_END)
        self.pages: list[bytearray | None] = [None] * self.TABLE_MAX_PAGES

    def get_unused_page_num(self):
        for i, page in enumerate(self.pages[1:], start=1):
            if page is None:
                return i
        
        raise Exception("Memory full")

    def get_page(self, page_num: int):
        if page_num > self.TABLE_MAX_PAGES:
            print(f"Tried to fetch page number out of bounds. {page_num} > {self.TABLE_MAX_PAGES}")
            sys.exit(1)

        if self.pages[page_num] is None:
            page = bytearray(self.PAGE_SIZE)
            number_of_ages_in_file = self.file_length // self.PAGE_SIZE

            if page_num <= number_of_ages_in_file:
                # set file pointer
                os.lseek(self.file_descriptor, page_num * self.PAGE_SIZE, os.SEEK_SET)
                # read single page from file
                bytes_read = os.read(self.file_descriptor, self.PAGE_SIZE)

                if len(bytes_read) == 0:
                    return None

                # put page bytes in page object
                page[:len(bytes_read)] = bytes_read

            # store page in cache
            self.pages[page_num] = page

        return self.pages[page_num]
    
    def set_page(self, page_num, data: bytes):
        if page_num >= self.TABLE_MAX_PAGES:
            raise IndexError(f"Tried to set page number out of bounds. {page_num} >= {self.TABLE_MAX_PAGES}")

        if len(data) > self.PAGE_SIZE:
            raise ValueError(f"Data size exceeds page size. {len(data)} > {self.PAGE_SIZE}")

        page = bytearray(self.PAGE_SIZE)
        page[:len(data)] = data
        self.pages[page_num] = page
        self._flush(page_num)

    def _flush(self, page_num):
        if self.pages[page_num] is None:
            print("Tried to flush null page")
            sys.exit(1)

        os.lseek(self.file_descriptor, page_num * self.PAGE_SIZE, os.SEEK_SET)
        os.write(self.file_descriptor, self.pages[page_num][:self.PAGE_SIZE])
