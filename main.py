from typing import List, Tuple
import sys
import random


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
        statement = str(tid) + " requests " + "S" if is_s_lock else "X" + " lock on " + str(k) + ": " + "G" if granted else "D"
        print(statement)
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

    # a transaction is finished if the length of its commands is 0
    def finished(self) -> bool:
        return len(self.commands) == 0


# set up the program, including creating a database, reading transaction files, and creating transactions
def setup(item_count: int, transaction_files: List[str]):
    # create database
    DB = Database(item_count, True)
    transactions = []
    # read transactions and add them to an array of transactions
    for file_name in transaction_files:
        transaction = read_transaction_file(file_name)
        transactions.append(transaction)
    return DB, transactions


def read_transaction_file(file_name: str) -> Transaction:
    with open(file_name) as file:
        for i, line in enumerate(file):
            if i == 0:
                local_variable_count = line.strip().split(" ")[1]
                local_variable_count = int(local_variable_count)
                transaction = Transaction(local_variable_count)
            else:
                operator, operand1, operand2 = line.strip().split(" ")
                transaction.add_command(operator, int(operand1), int(operand2))
    return transaction


# determines if the program can still process more transactions and can therefore proceed
def processing(transactions: List[Transaction]) -> bool:
    for transaction in transactions:
        if not transaction.finished():
            return True
    return False


if __name__ == '__main__':
    DB, transactions = setup(int(sys.argv[1]), sys.argv[2:])
    while processing(transactions):
        # randomly pick a transaction
        curr_transaction_index = random.randrange(0, len(transactions))
        # process the next instruction
        curr_transaction = transactions[curr_transaction_index]
        break