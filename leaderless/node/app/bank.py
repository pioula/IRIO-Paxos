from enum import IntEnum

from fastapi import HTTPException
from pydantic import BaseModel

from app.database import *

NODE_ID = os.environ["NODE_ID"]
db = connect()


class BankOpType(IntEnum):
    OPEN_ACCOUNT = 1
    DEPOSIT = 2
    WITHDRAW = 3
    TRANSFER = 4


class BankOperation(BaseModel):
    node_id: int = NODE_ID
    op_type: BankOpType = None
    args: dict = {}

    def __init__(self, op_type, args: dict, node_id: int = NODE_ID) -> None:
        super().__init__()
        self.op_type = op_type
        self.args = args
        self.node_id = node_id

    def __eq__(self, other):
        return isinstance(other, BankOperation) and self.dict() == other.dict()

    def __str__(self) -> str:
        return self.__repr__()


def get_account_with_id(cur, id: str):
    account = read_query(cur, "SELECT * FROM accounts WHERE id = \'{}\';".format(id))
    if len(account) == 0:
        db.rollback()
        raise HTTPException(status_code=404, detail="Account not found.")
    return {"id": id, "balance": account[0][1]}


def set_funds(cur, account):
    if account["balance"] < 0:
        db.rollback()
        raise HTTPException(status_code=403, detail="Not sufficient funds.")
    write_query(cur, "UPDATE accounts SET balance = {} WHERE id = \'{}\';" \
                .format(account["balance"], account["id"]))


def open_bank_account(id: int):
    with db.cursor() as cur:
        write_query(cur, "INSERT INTO accounts(id, balance) VALUES (\'{}\', 0);".format(id))
        db.commit()
    return {"id": id, "balance": 0}


def deposit_funds(id: str, amount: int):
    with db.cursor() as cur:
        account = get_account_with_id(cur, id)
        account["balance"] += amount
        set_funds(cur, account)
        db.commit()
    return account


def withdraw_funds(id: str, amount: int):
    with db.cursor() as cur:
        account = get_account_with_id(cur, id)
        account["balance"] -= amount
        set_funds(cur, account)
        db.commit()
    return account


def transfer_funds(to_id: str, from_id: str, amount: int):
    with db.cursor() as cur:
        account_from = get_account_with_id(cur, from_id)
        account_from["balance"] -= amount
        account_to = get_account_with_id(cur, to_id)
        account_to["balance"] += amount
        set_funds(cur, account_from)
        set_funds(cur, account_to)
        db.commit()
    return {"account_from": account_from, "account_to": account_to}


def execute(op: BankOperation, op_seq_num: id) -> dict:
    match op.op_type:
        case BankOpType.OPEN_ACCOUNT:
            return open_bank_account(op_seq_num)
        case BankOpType.DEPOSIT:
            return deposit_funds(**op.args)
        case BankOpType.WITHDRAW:
            return withdraw_funds(**op.args)
        case BankOpType.TRANSFER:
            return transfer_funds(**op.args)
        case _:
            raise ValueError("Operation type unrecognised!")

