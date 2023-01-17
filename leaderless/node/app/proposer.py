import logging
import os
import pickle
import random
from time import sleep
from fastapi import HTTPException
import requests

import bank as bank
import acceptor as acceptor

FILE_NAME = "proposer.pickle"

logger = logging.getLogger('PROPOSER')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('proposer.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:    %(name)s:    %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


NODES = 5
NODE_ID = int(os.environ["NODE_ID"])

EXP_BACKOFF_MULTIPLIER = 2


class NotEnoughNodesAvailable(Exception):
    """
    Raised when there are not enough
    paxos nodes available to reach consensus.
    """
    pass


def backoff(retries: int):
    backoff = random.randint(0, 2**retries - 1)
    logger.debug(f"Retrying after {backoff} seconds.")
    sleep(backoff)


def next_unique(number: int):
    return (number // NODES + 1) * NODES + NODE_ID


class Proposer:
    run_id: int

    def __init__(self):
        try:
            with open(FILE_NAME, "rb") as infile:
                on_disk_proposer = pickle.load(infile)
                self.run_id = on_disk_proposer.run_id

        except FileNotFoundError:
            self.run_id = 0

    def serialize(self):
        with open(FILE_NAME, "wb") as outfile:
            pickle.dump(self, outfile)

    def log(self, propose_id: int, message: str):
        logger.debug(f"[RUN: {self.run_id}] [PROPOSE_ID: {propose_id}]:    {message}")

    def broadcast_prepare(self, propose_id: int):
        mess = acceptor.PrepareMessage(run_id=self.run_id, propose_id=propose_id)
        responses = []

        self.log(propose_id, f"Broadcasting: {mess}")

        for id in range(1, NODES + 1):
            if id == NODE_ID:
                r = acceptor.acceptor_prepare(mess)
            else:
                try:
                    r = requests.put(f"http://node{id}:80/acceptor_prepare", json=mess.dict())
                    r = r.json()
                except Exception as error:
                    self.log(propose_id, f"Sending PREPARE to node {id} failed. Reason: {error}")
                    continue
            responses.append(r)

        self.log(propose_id, f"Received PROMISEs: {responses}")

        max_accepted_id = -1
        max_promised_id = -1
        res = {"accepted_id": -1, "accepted_val": None}
        for r in responses:
            if "promised_id" in r:
                max_promised_id = max(max_promised_id, r["promised_id"])
                res = r
            elif max_promised_id == -1 and r["accepted_id"] > max_accepted_id:
                max_accepted_id = r["accepted_id"]
                res = r

        if len(responses) <= NODES // 2:
            self.log(propose_id, f"Majority of nodes did not respond to prepare message. Responses count: {len(responses)}")
            return None
        return res

    def broadcast_accept(self, propose_id: int, val: bank.BankOperation) -> bool:
        mess: acceptor.AcceptMessage = acceptor.AcceptMessage(run_id=self.run_id, propose_id=propose_id, val=val)
        accepts_cnt = 0

        self.log(propose_id, f"Broadcasting {mess}")

        for id in range(1, NODES + 1):
            if id == NODE_ID:
                r = acceptor.acceptor_accept(mess)
            else:
                try:
                    r = requests.put(f"http://node{id}:80/acceptor_accept", json=mess.dict())
                    r = r.json()
                except Exception as error:
                    self.log(propose_id, f"Sending ACCEPT to node {id} failed: {error}")
                    continue
            if r["accepted"]:
                accepts_cnt += 1

        self.log(propose_id, f"{accepts_cnt} nodes accepted value {val}.")

        return accepts_cnt > NODES // 2

    def paxos(self, op: bank.BankOperation) -> bank.BankOperation:
        propose_id = NODE_ID
        retries = 0
        while True:
            self.log(propose_id, f"Proposing {op}")
            res = self.broadcast_prepare(propose_id=propose_id)
            if res is None:
                propose_id = next_unique(propose_id + 1)
                backoff(retries)
                retries += 1
                continue
            if "promised_id" in res:
                self.log(propose_id, f"Received NACK response {res}.")
                propose_id = next_unique(res["promised_id"])
                backoff(retries)
                retries += 1
                continue
            elif res["accepted_val"] is None:
                self.log(propose_id, f"Majority of nodes have NOT accepted any value yet.")
                bank.validate_without_executing(op)
                pass
            else:
                self.log(propose_id, f"Majority of nodes accepted value: {res}. ")
                op = bank.BankOperation(**res["accepted_val"])

            accepted = self.broadcast_accept(propose_id=propose_id, val=op)
            if accepted:
                self.log(propose_id, f"Operation {op} was accepted by majority of nodes.")
                return op
            else:
                self.log(propose_id, f"Operation {op} was NOT accepted by majority of nodes.")
                propose_id += NODES
                backoff(retries)
                retries += 1

    def execute(self, my_op: bank.BankOperation) -> dict:
        op = bank.BankOperation(op_type=None, args={})
        res = {}
        while op != my_op:
            op = self.paxos(my_op.copy())
            res = bank.execute(op, self.run_id)
            self.run_id += 1
            self.serialize()
        return res
