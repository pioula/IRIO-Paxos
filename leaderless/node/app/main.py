import os
from enum import Enum
from typing import Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.database import *
from app.acceptor import Acceptor

app = FastAPI()
db = connect()
acceptor = Acceptor()
NODE_ID = os.environ["NODE_ID"]


class UpdateBalance(BaseModel):
    id: str
    amount: int = Field(ge=0)


class Transfer(BaseModel):
    from_id: str
    to_id: str
    amount: int = Field(ge=0)


class BankOpType(Enum):
    OPEN_ACCOUNT = 1
    DEPOSIT = 2
    WITHDRAW = 3
    TRANSFER = 4


class BankOperation(BaseModel):
    node_id: int = NODE_ID
    op_type: BankOpType = None
    args: Union[None, UpdateBalance, Transfer] = None

    def __init__(self, op_type: BankOpType, args: Union[None, UpdateBalance, Transfer]) -> None:
        super().__init__()
        self.op_type = op_type
        self.args = args


class PrepareMessage(BaseModel):
    run_id: int
    propose_id: int


class AcceptMessage(BaseModel):
    run_id: int
    propose_id: int
    val: BankOperation


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


@app.get("/health")
def healthcheck():
    return {"healthy": "true"}


def open_bank_account(id: int):
    with db.cursor() as cur:
        write_query(cur, "INSERT INTO accounts(id, balance) VALUES (\'{}\', 0);".format(id))
        db.commit()
    return {"id": id, "balance": 0}


def deposit_funds(body: UpdateBalance):
    with db.cursor() as cur:
        account = get_account_with_id(cur, body.id)
        account["balance"] += body.amount
        set_funds(cur, account)
        db.commit()
    return account


def withdraw_funds(body: UpdateBalance):
    with db.cursor() as cur:
        account = get_account_with_id(cur, body.id)
        account["balance"] -= body.amount
        set_funds(cur, account)
        db.commit()
    return account


def transfer_funds(body: Transfer):
    with db.cursor() as cur:
        account_from = get_account_with_id(cur, body.from_id)
        account_from["balance"] -= body.amount
        account_to = get_account_with_id(cur, body.to_id)
        account_to["balance"] += body.amount
        set_funds(cur, account_from)
        set_funds(cur, account_to)
        db.commit()
    return {"account_from": account_from, "account_to": account_to}


@app.put("/acceptor_prepare")
def acceptor_prepare(body: PrepareMessage):
    res = acceptor.handle_prepare(body.run_id, body.propose_id)
    acceptor.serialize()
    if res[0] is not None:
        # Prepare operation succeeded!
        return {"accepted_id": res[0], "accepted_val": res[1]}
    # Prepare operation did not succeed, send NACK response.
    return {"promised_id": res[1]}


@app.put("/acceptor_accept")
def acceptor_accept(body: AcceptMessage):
    res = acceptor.handle_accept(body.run_id, body.propose_id, body.val)
    acceptor.serialize()
    return {"accepted": res}


@app.post("/open")
def paxos_open_bank_account():
    op = BankOperation(op_type=BankOpType.OPEN_ACCOUNT, args=None)
    return paxos_execute(op)


@app.put("/deposit")
def paxos_deposit_funds(body: UpdateBalance):
    op = BankOperation(op_type=BankOpType.DEPOSIT, args=body)
    return paxos_execute(op)


@app.put("/withdraw")
def paxos_withdraw_funds(body: UpdateBalance):
    op = BankOperation(op_type=BankOpType.WITHDRAW, args=body)
    return paxos_execute(op)


@app.put("/transfer")
def paxos_transfer_funds(body: Transfer):
    op = BankOperation(op_type=BankOpType.TRANSFER, args=body)
    return paxos_execute(op)


def paxos(run_id: int, op: BankOperation) -> BankOperation:
    # TODO
    pass


def execute(op: BankOperation, run_id: int):
    match op.op_type:
        case BankOpType.OPEN_ACCOUNT:
            return open_bank_account(run_id)
        case BankOpType.DEPOSIT:
            return deposit_funds(op.args)
        case BankOpType.WITHDRAW:
            return withdraw_funds(op.args)
        case BankOpType.TRANSFER:
            return transfer_funds(op.args)
        case _:
            raise ValueError("Operation type unrecognised!")


def paxos_execute(op: BankOperation) -> dict:
    # TODO
    return execute(op=op, run_id=0)
