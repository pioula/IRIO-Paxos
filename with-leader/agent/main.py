import uuid
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from database import *

app = FastAPI()
db = connect()

class UpdateBalance(BaseModel):
  id: str
  amount: int = Field(ge=0)

class Transfer(BaseModel):
  from_id: str
  to_id: str
  amount: int = Field(ge=0)

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

@app.post("/open")
def open_bank_account():
  id = uuid.uuid4()
  with db.cursor() as cur:
    write_query(cur, "INSERT INTO accounts(id, balance) VALUES (\'{}\', 0);".format(id))
    db.commit()
  return {"id": id, "balance": 0}

@app.put("/deposit")
def deposit_funds(body: UpdateBalance):
  with db.cursor() as cur:
    account = get_account_with_id(cur, body.id)
    account["balance"] += body.amount
    set_funds(cur, account)
    db.commit()
  return account

@app.put("/withdraw") 
def withdraw_funds(body: UpdateBalance):
  with db.cursor() as cur:
    account = get_account_with_id(cur, body.id)
    account["balance"] -= body.amount
    set_funds(cur, account)
    db.commit()
  return account

@app.put("/transfer")
def transfer_funds(body: Transfer):
  with db.cursor() as cur:
    account_from = get_account_with_id(cur, body.from_id)
    account_to = get_account_with_id(cur, body.to_id)
    account_from["balance"] -= body.amount
    account_to = get_account_with_id(cur, body.to_id)
    account_to["balance"] += body.amount
    set_funds(cur, account_from)
    set_funds(cur, account_to)
    db.commit()
  return {"account_from": account_from, "account_to": account_to}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)