from pathlib import Path
import random

from constants import FILENAME
from row import Row
from table import Table

# List of sample first names and last names
first_names = ["Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Henry", "Ivy", "Jack"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

# List of sample email domains
email_domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"]

def generate_random_name():
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def generate_random_email(name):
    username = name.lower().replace(" ", ".")
    domain = random.choice(email_domains)
    return f"{username}@{domain}"

def seed_database(num_entries=10):
    table = Table()

    for i in range(num_entries):
        name = generate_random_name()
        email = generate_random_email(name)
        row = Row(i, name, email)
        print(f"inserting {row}")
        table.insert_row(row)

    print(f"Seeded database with {num_entries} entries.")
    table.save_btree()
    print("Database saved.")

if __name__ == "__main__":
    Path(FILENAME).unlink()
    seed_database()