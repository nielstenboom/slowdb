import os
import pytest
from pager import Pager

@pytest.fixture
def temp_db_file(tmp_path):
    db_file = tmp_path / "test_storage.slowdb"
    yield str(db_file)

def test_pager_happy_path(temp_db_file):
    # Initialize the Pager
    pager = Pager(filename=temp_db_file)

    # Test setting a page
    test_data = b"Hello, World!" * 300  # 3900 bytes
    pager.set_page(0, test_data)

    # Test getting the same page
    retrieved_page = pager.get_page(0)
    assert retrieved_page[:len(test_data)] == test_data
    assert len(retrieved_page) == Pager.PAGE_SIZE

    # Test setting and getting a different page
    test_data_2 = b"Another test" * 200  # 2400 bytes
    pager.set_page(1, test_data_2)
    retrieved_page_2 = pager.get_page(1)
    assert retrieved_page_2[:len(test_data_2)] == test_data_2
    assert len(retrieved_page_2) == Pager.PAGE_SIZE

    # Verify that the first page is still intact
    retrieved_page_1_again = pager.get_page(0)
    assert retrieved_page_1_again[:len(test_data)] == test_data

    # Test reading from file after reopening
    del pager
    pager = Pager(filename=temp_db_file)
    retrieved_page_1_after_reopen = pager.get_page(0)
    retrieved_page_2_after_reopen = pager.get_page(1)
    assert retrieved_page_1_after_reopen[:len(test_data)] == test_data
    assert retrieved_page_2_after_reopen[:len(test_data_2)] == test_data_2