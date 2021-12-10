from typing import List, Tuple, Dict
import sys
import random

# README!!!
# To run this program, do
# python main.py <number of elements in the database> <file 1> <file 2> ...


class Database:
    def __init__(self, k: int, nonzero: bool):
        # database to store k integers
        self.database = [i + 1 for i in range(k)] if nonzero else [0] * k

    def read(self, k: int) -> int:
        return self.database[k]

    def write(self, k: int, w: int) -> None:
        self.database[k] = w

    def print(self) -> None:
        print("Database:", self.database)


class LockManager:
    def __init__(self, DB: Database):
        # We need to create a lock table based on the data items in the DB
        # {data item: list of transaction ids holding the lock on this data item}
        self.lock_table: Dict[int, List[int]] = {i: [] for i in range(len(DB.database))}
        # Also create a table to see which transaction has which locks to make it more efficient
        # it's a dictionary where each tid has a list of tuples: int contains the index to be locked,
        # bool is whether it's an S-Lock: True for S-lock, False for X-lock
        self.transaction_locks: Dict[int, List[Tuple[int, bool]]] = {}

    # transaction id, kth integer of the database, if the request is for a S-lock
    # return 1 if lock is granted, 0 if not
    def request(self, tid: int, k: int, is_s_lock: bool) -> int:
        granted = False
        # if we can grant this lock, we need to first release all lock held previously by this tid
        if is_s_lock:
            # if no other transaction has an X-lock
            if not self.has_x_lock_on(k, tid):
                granted = True
                self.lock_table[k].append(tid)
                self.transaction_locks.setdefault(tid, []).append((k, True))
        else:
            # if it's an x-lock, we need to first check if any other transactions has any lock on k
            if not self.has_lock_on(k, tid):
                granted = True
                # see if the current tid holds a lock on k already
                if tid in self.lock_table[k]:
                    for i, (curr_k, curr_is_s_lock) in enumerate(self.transaction_locks[tid]):
                        if curr_k == k:
                            # update to an X-lock
                            self.transaction_locks[tid][i] = (k, False)
                # if the current tid does not hold a lock on k
                else:
                    self.lock_table[k].append(tid)
                    self.transaction_locks.setdefault(tid, []).append((k, False))
        statement = "T" + str(tid) + " request " + ("S" if is_s_lock else "X") + "-lock on item " + str(
            k) + ": " + ("G" if granted else "D")
        print(statement)
        return 1 if granted else 0

    # release all locks held by transaction tid
    # return the number of locks released
    def releaseAll(self, tid: int) -> int:

        if tid not in self.transaction_locks:
            return 0
        locks_released = len(self.transaction_locks[tid])
        del self.transaction_locks[tid]
        for item, locks in self.lock_table.items():
            if tid in locks:
                locks.remove(tid)
                self.lock_table[item] = locks
        return locks_released

    # return all locks held by tid
    def showLocks(self, tid: int) -> List[Tuple[int, bool]]:
        locks_held = self.transaction_locks[tid] if tid in self.transaction_locks else []
        return locks_held

    # shows if a data item has any lock on it
    def has_lock_on(self, item: int, tid: int) -> bool:
        for id in self.lock_table[item]:
            # if there exists any other tids that hold this item and are not the tid, return True
            if id != tid:
                return True
        return False

    # shows if a data item has any x-lock on it
    def has_x_lock_on(self, item: int, tid: int) -> bool:
        transactions = self.lock_table[item]
        for id, transaction in enumerate(transactions):
            # we're only checking other transactions so we need to ignore the current one
            if tid != id:
                locks = self.transaction_locks[transaction]
                for data, is_s_lock in locks:
                    if not is_s_lock:
                        return True
        return False


class Transaction:
    def __init__(self, k: int):
        self.local = [i for i in range(k)]
        self.commands = []

    # read the source-th number from the db and set it local[dest]
    def read(self, db: Database, source: int, dest: int) -> None:
        # read db[x] and store it to local[y]
        print("read db[", source, "] and store it to local[", dest, "]", sep='')
        self.local[dest] = db.read(source)

    # write local[source] to the dest-th number in the db
    def write(self, db: Database, source: int, dest: int) -> None:
        # write local[x] to db[y]
        print("write local[", source, "] to db[", dest, "]", sep='')
        db.write(dest, self.local[source])

    def add(self, source: int, v: int) -> None:
        # local[x] = local[x] + d
        print("local[", source, "] = local[", source, "] + ", v, sep='')
        self.local[source] += v

    def sub(self, source: int, v: int) -> None:
        print("local[", source, "] = local[", source, "] - ", v, sep='')
        self.local[source] -= v

    def mult(self, source: int, v: int) -> None:
        print("local[", source, "] = local[", source, "] * ", v, sep='')
        self.local[source] *= v

    def copy(self, s1: int, s2: int) -> None:
        # local[x] = local[y]
        print("local[", s1, "] = local[", s2, "] + ", sep='')
        self.local[s1] = self.local[s2]

    def combine(self, s1: int, s2: int) -> None:
        # local[x] = local[x] + local[y]
        print("local[", s1, "] = local[", s1, "] + local[", s2, "]", sep='')
        self.local[s1] += self.local[s2]

    def display(self) -> None:
        for num in self.local:
            print(num, end=' ')
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
    Manager = LockManager(DB)
    transactions = []
    # read transactions and add them to an array of transactions
    for file_name in transaction_files:
        transaction = read_transaction_file(file_name)
        transactions.append(transaction)
    return DB, transactions, Manager


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


def print_transactions(transactions: List[Transaction]) -> None:
    for transaction in transactions:
        transaction.display()


# performs the next command and removes it from the list
# returns true if this command is allowed to proceed
def do_next_command(db: Database, manager: LockManager, transaction: Transaction, tid: int):
    operator, operand1, operand2 = transaction.commands[0]
    if operator == 'R':
        granted = manager.request(tid, operand1, True)
    elif operator == 'W':
        granted = manager.request(tid, operand2, False)
    else:
        granted = True

    if granted:
        transaction.commands.pop(0)

    # read
    if operator == 'R':
        if granted:
            print("T" + str(curr_transaction_index) + " execute ", end='')
            transaction.read(db, operand1, operand2)
    # write
    elif operator == 'W':
        if granted:
            print("T" + str(curr_transaction_index) + " execute ", end='')
            transaction.write(db, operand1, operand2)
    # add
    elif operator == 'A':
        print("T" + str(curr_transaction_index) + " execute ", end='')
        transaction.add(operand1, operand2)
    # subtract
    elif operator == 'S':
        print("T" + str(curr_transaction_index) + " execute ", end='')
        transaction.sub(operand1, operand2)
    # multiply
    elif operator == 'M':
        print("T" + str(curr_transaction_index) + " execute ", end='')
        transaction.mult(operand1, operand2)
    # copy
    elif operator == 'C':
        print("T" + str(curr_transaction_index) + " execute ", end='')
        transaction.copy(operand1, operand2)
    # combine
    elif operator == 'O':
        print("T" + str(curr_transaction_index) + " execute ", end='')
        transaction.combine(operand1, operand2)
    # print the current elements in the database
    elif operator == 'P':
        db.print()

    return granted


# detect deadlock
def no_deadlock(deadlocks: List[bool], granted: bool, transactions: List[Transaction], tid: int) -> bool:
    # first, refresh to check if there are any transactions finished; if finished, their deadlock needs to be true
    for i, transaction in enumerate(transactions):
        if transaction.finished():
            deadlocks[i] = True

    # if the current transaction is granted, set all the deadlocks to False
    if granted:
        deadlocks = [False] * len(deadlocks)
    # otherwise, set the deadlocks[tid] to True
    else:
        deadlocks[tid] = True
    # check if everything is true
    # if there's even one False in the array, there is no deadlock
    return False in deadlocks


def print_locks(transactions: List[Transaction], manager: LockManager):
    for tid, transaction in enumerate(transactions):
        print(manager.showLocks(tid))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise "No command line argument!"
    DB, transactions, Manager = setup(int(sys.argv[1]), sys.argv[2:])
    deadlocks = [False] * len(transactions)
    while processing(transactions):
        # randomly pick a transaction
        curr_transaction_index = random.randrange(0, len(transactions))
        # process the next instruction
        curr_transaction = transactions[curr_transaction_index]
        if not curr_transaction.finished():
            granted = do_next_command(DB, Manager, curr_transaction, curr_transaction_index)
            if not no_deadlock(deadlocks, granted, transactions, curr_transaction_index):
                print("Deadlock")
                # print_locks(transactions, Manager)
                break
        # if the current transaction is finished, release all locks
        if curr_transaction.finished():
            Manager.releaseAll(curr_transaction_index)
    DB.print()
