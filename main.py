import sys
from enum import Enum, auto

class InputBuffer:
    buffer: str
    buffer_length: int
    input_length: int

    def __init__(self):
        self.buffer = ""
        self.buffer_length = 0
        self.input_length = 0

class MetaCommandResult(Enum):
    SUCCESS = auto()
    UNRECOGNIZED_COMMAND = auto()

class PrepareResult(Enum):
    SUCCESS = auto()
    UNRECOGNIZED_STATEMENT = auto()

class StatementType(Enum):
    INSERT = auto()
    SELECT = auto()

class Statement:
    def __init__(self):
        self.type = None

def new_input_buffer() -> InputBuffer:
    return InputBuffer()

def print_prompt() -> None:
    print("slowestdbintheworld > ", end="", flush=True)

def read_input(input_buffer: InputBuffer) -> None:
    try:
        input_buffer.buffer = input()
        input_buffer.input_length = len(input_buffer.buffer)
        input_buffer.buffer_length = input_buffer.input_length
    except EOFError:
        print("Error reading input")
        sys.exit(1)


def do_meta_command(input_buffer: InputBuffer) -> MetaCommandResult:
    if input_buffer.buffer == ".exit":
        sys.exit(0)
    else:
        return MetaCommandResult.UNRECOGNIZED_COMMAND

def prepare_statement(input_buffer: InputBuffer, statement: Statement) -> PrepareResult:
    if input_buffer.buffer.startswith("insert"):
        statement.type = StatementType.INSERT
        return PrepareResult.SUCCESS
    if input_buffer.buffer == "select":
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS

    return PrepareResult.UNRECOGNIZED_STATEMENT

def execute_statement(statement: Statement) -> None:
    if statement.type == StatementType.INSERT:
        print("This is where we would do an insert.")
    elif statement.type == StatementType.SELECT:
        print("This is where we would do a select.")


def eval_loop(input_buffer: InputBuffer) -> None:

    # handle meta commands
    # all commands that start with a dot are meta commands, they're not SQL statements
    if input_buffer.buffer.startswith('.'):
        meta_command_result = do_meta_command(input_buffer)
        if meta_command_result == MetaCommandResult.SUCCESS:
            return
        elif meta_command_result == MetaCommandResult.UNRECOGNIZED_COMMAND:
            print(f"Unrecognized command '{input_buffer.buffer}'")
            return

    # handle SQL statements
    statement = Statement()
    prepare_result = prepare_statement(input_buffer, statement)
    if prepare_result == PrepareResult.SUCCESS:
        execute_statement(statement)
        print("Executed.")
    elif prepare_result == PrepareResult.UNRECOGNIZED_STATEMENT:
        print(f"Unrecognized keyword at start of '{input_buffer.buffer}'.")
        return

def main() -> None:
    input_buffer = new_input_buffer()
    while True:
        print_prompt()
        read_input(input_buffer)
        eval_loop(input_buffer)

if __name__ == "__main__":
    main()