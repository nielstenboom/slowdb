import struct


class Row:
    COLUMN_USERNAME_SIZE = 32
    COLUMN_EMAIL_SIZE = 255
    ROW_SIZE = struct.calcsize(f"I{COLUMN_USERNAME_SIZE}s{COLUMN_EMAIL_SIZE}s")


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

