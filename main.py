import sys
from enum import Enum, auto

from db import ExecuteResult, Row, Table, db_close

class InputBuffer:
    def __init__(self):
        self.buffer = ""
        self.buffer_length = 0
        self.input_length = 0

class MetaCommandResult(Enum):
    SUCCESS = auto()
    UNRECOGNIZED_COMMAND = auto()

class PrepareResult(Enum):
    SUCCESS = auto()
    SYNTAX_ERROR = auto()
    UNRECOGNIZED_STATEMENT = auto()

class StatementType(Enum):
    INSERT = auto()
    SELECT = auto()

class Statement:
    def __init__(self):
        self.type = None
        self.row_to_insert = None  # This will store the row data for the insert statement

def do_meta_command(input_buffer: InputBuffer, table: Table) -> MetaCommandResult:
    if input_buffer.buffer == ".exit":
        db_close(table)
        sys.exit(0)
    return MetaCommandResult.UNRECOGNIZED_COMMAND

def prepare_statement(input_buffer: InputBuffer, statement: Statement) -> PrepareResult:
    if input_buffer.buffer.startswith("insert"):
        parts = input_buffer.buffer.split()
        if len(parts) < 4:
            return PrepareResult.SYNTAX_ERROR
        try:
            row_id = int(parts[1])
            username = parts[2]
            email = parts[3]
        except ValueError:
            return PrepareResult.SYNTAX_ERROR
        statement.type = StatementType.INSERT
        statement.row_to_insert = Row(row_id, username, email)
        return PrepareResult.SUCCESS
    
    if input_buffer.buffer == "select":
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS
    
    return PrepareResult.UNRECOGNIZED_STATEMENT

# DB 'virtual machine'
def execute_statement(statement: Statement, table: Table) -> ExecuteResult:
    if statement.type == StatementType.INSERT:
        return table.insert_row(statement.row_to_insert)
    elif statement.type == StatementType.SELECT:
        return table.select_all()
    
    return ExecuteResult.SUCCESS


def read_input(input_buffer: InputBuffer) -> None:
    try:
        input_buffer.buffer = input()
        input_buffer.input_length = len(input_buffer.buffer)
        input_buffer.buffer_length = input_buffer.input_length
    except EOFError:
        print("Error reading input")
        sys.exit(1)


def eval_loop(input_buffer: InputBuffer, table: Table) -> None:
    # handle meta commands like ".exit"
    if input_buffer.buffer.startswith('.'):
        meta_command_result = do_meta_command(input_buffer, table)
        if meta_command_result == MetaCommandResult.SUCCESS:
            return
        elif meta_command_result == MetaCommandResult.UNRECOGNIZED_COMMAND:
            print(f"Unrecognized command '{input_buffer.buffer}'")
            return


    # handle SQL statements
    # this part is a very simple SQL compiler
    # in real life db the Statement would be bytecode that's passed to the db virtual machine
    statement = Statement()
    prepare_result = prepare_statement(input_buffer, statement)
    
    # abort early if the statement is not valid
    if prepare_result == PrepareResult.SYNTAX_ERROR:
        print("Syntax error. Could not parse statement.")
        return
    elif prepare_result == PrepareResult.UNRECOGNIZED_STATEMENT:
        print(f"Unrecognized keyword at start of '{input_buffer.buffer}'.")
        return 


    # pass the statement to the db virtual machine if the statement is valid
    execute_result = execute_statement(statement, table)
    if execute_result == ExecuteResult.SUCCESS:
        print("")
    elif execute_result == ExecuteResult.TABLE_FULL:
        print("Error: Table full.")


def main() -> None:
    table = Table()
    input_buffer = InputBuffer()
    while True:
        print("slowestdbintheworld > ", end="", flush=True)
        read_input(input_buffer)
        eval_loop(input_buffer, table)

if __name__ == "__main__":
    main()
