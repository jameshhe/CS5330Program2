from typing import List, Tuple
import sys


class Database:
    def __init__(self, k: int, nonzero: bool):
        # database to store k integers
        self.database = [i + 1 for i in range(k)] if nonzero else [0] * k

    def read(self, k: int) -> int:
        return self.database[k]

    def write(self, k: int, w: int) -> None:
        self.database[k] = w

    def print(self) -> None:
        print(self.database)


class LockManager:
    def __init__(self):
        self.locks = {}

    # transaction id, kth integer of the database, if the request is for a S-lock
    # return 1 if lock is granted, 0 if not
    def request(self, tid: int, k: int, is_s_lock: bool) -> int:
        granted = False
        return 1 if granted else 0

    # release all locks held by transaction tid
    # return the number of locks released
    def releaseAll(self, tid: int) -> int:
        locks_released = 0
        return locks_released

    # return all locks held by tid
    def showLocks(self, tid: int) -> List[Tuple[int, bool]]:
        pass


class Transaction:
    def __init__(self, k: int):
        self.local = [i for i in range(k)]
        self.commands = []

    # read the source-th number from the db and set it local[dest]
    def read(self, db: Database, source: int, dest: int) -> None:
        self.local[dest] = db.read(source)

    # write local[source] to the dest-th number in the db
    def write(self, db: Database, source: int, dest: int) -> None:
        db.write(dest, self.local[source])

    def add(self, source: int, v: int) -> None:
        self.local[source] += v

    def sub(self, source: int, v: int) -> None:
        self.local[source] -= v

    def mult(self, source: int, v: int) -> None:
        self.local[source] *= v

    def copy(self, s1: int, s2: int) -> None:
        self.local[s1] = self.local[s2]

    def combine(self, s1: int, s2: int) -> None:
        self.local[s1] += self.local[s2]

    def display(self) -> None:
        print("Local Database:")
        for num in self.local:
            print(num, end=' ')
        print("\nCommands:")
        for command in self.commands:
            print(command, end=' ')
        print()

    def add_command(self, operator, operand1, operand2):
        self.commands.append([operator, operand1, operand2])


def read_transaction_file(file_name: str, item_count: int) -> Transaction:
    transaction = Transaction(item_count)
    with open(file_name) as file:
        for i, line in enumerate(file):
            if i != 0:
                operator, operand1, operand2 = line.strip().split(" ")
                transaction.add_command(operator, int(operand1), int(operand2))
    return transaction


if __name__ == '__main__':
    item_count, transaction_files = int(sys.argv[1]), sys.argv[2:]
    # create database
    DB = Database(item_count, True)
    transactions = []
    for file_name in transaction_files:
        transaction = read_transaction_file(file_name, item_count)
        transactions.append(transaction)
    for transaction in transactions:
        transaction.display()