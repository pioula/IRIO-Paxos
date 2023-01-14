from anyio import CapacityLimiter
from anyio.lowlevel import RunVar
from fastapi import FastAPI
from pydantic import BaseModel, Field
import uvicorn

import bank as bank
from proposer import Proposer
import acceptor as acceptor


app = FastAPI()
app.include_router(acceptor.router)
proposer = Proposer()

config = uvicorn.Config(app, host="0.0.0.0", port=80, log_level="info")
server = uvicorn.Server(config=config)

class UpdateBalance(BaseModel):
    id: str
    amount: int = Field(ge=0)


class Transfer(BaseModel):
    from_id: str
    to_id: str
    amount: int = Field(ge=0)


@app.on_event("startup")
def startup():
    RunVar("_default_thread_limiter").set(CapacityLimiter(1))


@app.get("/health")
def healthcheck():
    return {"healthy": "true"}


@app.post("/open")
def open_bank_account():
    op = bank.BankOperation(op_type=bank.BankOpType.OPEN_ACCOUNT, args={})
    return proposer.execute(op)


@app.put("/deposit")
def deposit_funds(body: UpdateBalance):
    op = bank.BankOperation(op_type=bank.BankOpType.DEPOSIT, args=body.dict())
    return proposer.execute(op)


@app.put("/withdraw")
def withdraw_funds(body: UpdateBalance):
    op = bank.BankOperation(op_type=bank.BankOpType.WITHDRAW, args=body.dict())
    return proposer.execute(op)


@app.put("/transfer")
def transfer_funds(body: Transfer):
    op = bank.BankOperation(op_type=bank.BankOpType.TRANSFER, args=body.dict())
    return proposer.execute(op)

@app.get("/quit")
def quit_app():
    global server
    server.should_exit = True
    server.force_exit = True
    server.shutdown()
    return {}

def main():
    global server
    server.run()

if __name__ == "__main__":
    main()
