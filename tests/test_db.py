from db import Row, Table, ExecuteResult
from main import prepare_statement, Statement, PrepareResult, StatementType, execute_statement

def test_row_serialization():
    row = Row(1, "user1", "user1@example.com")
    serialized = row.serialize()
    deserialized = Row.deserialize(serialized)
    assert deserialized.id == 1
    assert deserialized.username == "user1"
    assert deserialized.email == "user1@example.com"

def test_table_insert_and_select():
    table = Table()
    row1 = Row(1, "user1", "user1@example.com")
    row2 = Row(2, "user2", "user2@example.com")
    
    assert table.insert_row(row1) == ExecuteResult.SUCCESS
    assert table.insert_row(row2) == ExecuteResult.SUCCESS
    assert table.num_rows == 2

    table.select_all()

def test_insert_max_length_values():
    table = Table()
    max_username = "a" * Row.COLUMN_USERNAME_SIZE
    max_email = "b" * Row.COLUMN_EMAIL_SIZE
    
    row = Row(1, max_username, max_email)
    assert table.insert_row(row) == ExecuteResult.SUCCESS
    
    # Verify the inserted row
    inserted_row_data = table.row_memory_data(0)
    inserted_row = Row.deserialize(inserted_row_data)
    
    assert inserted_row.id == 1
    assert len(inserted_row.username) == Row.COLUMN_USERNAME_SIZE
    assert inserted_row.username == max_username
    assert len(inserted_row.email) == Row.COLUMN_EMAIL_SIZE
    assert inserted_row.email == max_email

def test_insert_overflow_values():
    table = Table()
    overflow_username = "a" * (Row.COLUMN_USERNAME_SIZE + 10)
    overflow_email = "b" * (Row.COLUMN_EMAIL_SIZE + 10)
    
    row = Row(1, overflow_username, overflow_email)
    assert table.insert_row(row) == ExecuteResult.SUCCESS
    
    # Verify the inserted row
    inserted_row_data = table.row_memory_data(0)
    inserted_row = Row.deserialize(inserted_row_data)
    
    assert inserted_row.id == 1
    assert len(inserted_row.username) == Row.COLUMN_USERNAME_SIZE
    assert inserted_row.username == overflow_username[:Row.COLUMN_USERNAME_SIZE]
    assert len(inserted_row.email) == Row.COLUMN_EMAIL_SIZE
    assert inserted_row.email == overflow_email[:Row.COLUMN_EMAIL_SIZE]


def test_prepare_statement():
    input_buffer = type('InputBuffer', (), {'buffer': 'insert 1 user1 user1@example.com'})()
    statement = Statement()
    result = prepare_statement(input_buffer, statement)
    assert result == PrepareResult.SUCCESS
    assert statement.type == StatementType.INSERT
    assert statement.row_to_insert.id == 1
    assert statement.row_to_insert.username == "user1"
    assert statement.row_to_insert.email == "user1@example.com"

    input_buffer.buffer = 'select'
    result = prepare_statement(input_buffer, statement)
    assert result == PrepareResult.SUCCESS
    assert statement.type == StatementType.SELECT

    input_buffer.buffer = 'invalid statement'
    result = prepare_statement(input_buffer, statement)
    assert result == PrepareResult.UNRECOGNIZED_STATEMENT

def test_execute_statement():
    table = Table()
    insert_statement = Statement()
    insert_statement.type = StatementType.INSERT
    insert_statement.row_to_insert = Row(1, "user1", "user1@example.com")
    
    result = execute_statement(insert_statement, table)
    assert result == ExecuteResult.SUCCESS
    assert table.num_rows == 1

    select_statement = Statement()
    select_statement.type = StatementType.SELECT
    
    result = execute_statement(select_statement, table)
    assert result == ExecuteResult.SUCCESS

def test_table_full():
    table = Table()
    for i in range(Table.TABLE_MAX_ROWS):
        row = Row(i, f"user{i}", f"user{i}@example.com")
        assert table.insert_row(row) == ExecuteResult.SUCCESS
    
    # Try to insert one more row
    extra_row = Row(Table.TABLE_MAX_ROWS, "extra", "extra@example.com")
    assert table.insert_row(extra_row) == ExecuteResult.TABLE_FULL