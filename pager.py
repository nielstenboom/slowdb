import os
import sys

class Pager:
    PAGE_SIZE = 4096
    TABLE_MAX_PAGES = 100

    def __init__(self, filename: str):
        self.file_descriptor = os.open(filename, os.O_RDWR | os.O_CREAT)
        self.file_length = os.lseek(self.file_descriptor, 0, os.SEEK_END)
        self.pages = [None] * self.TABLE_MAX_PAGES

    def get_page(self, page_num: int):
        if page_num > self.TABLE_MAX_PAGES:
            print(f"Tried to fetch page number out of bounds. {page_num} > {self.TABLE_MAX_PAGES}")
            sys.exit(1)

        if self.pages[page_num] is None:
            page = bytearray(self.PAGE_SIZE)
            number_of_ages_in_file = self.file_length // self.PAGE_SIZE

            # if page at end of the file is not fully filled yet, include it here
            is_partial_page_at_end = (self.file_length % self.PAGE_SIZE) > 0
            if is_partial_page_at_end:
                number_of_ages_in_file += 1

            if page_num <= number_of_ages_in_file:
                # set file pointer
                os.lseek(self.file_descriptor, page_num * self.PAGE_SIZE, os.SEEK_SET)
                # read single page from file
                bytes_read = os.read(self.file_descriptor, self.PAGE_SIZE)
                # put page bytes in page object
                page[:len(bytes_read)] = bytes_read

            # store page in cache
            self.pages[page_num] = page

        return self.pages[page_num]

    def flush(self, page_num, size):
        if self.pages[page_num] is None:
            print("Tried to flush null page")
            sys.exit(1)

        os.lseek(self.file_descriptor, page_num * self.PAGE_SIZE, os.SEEK_SET)
        os.write(self.file_descriptor, self.pages[page_num][:size])
