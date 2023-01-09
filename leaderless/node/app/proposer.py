import logging
import os
import pickle
import random
from time import sleep
from fastapi import HTTPException
import requests

import app.bank as bank
import app.acceptor as acceptor

FILE_NAME = "proposer.pickle"

logging.basicConfig()
logger = logging.getLogger("PROPOSER")
logger.setLevel(logging.DEBUG)

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
                self.run_id = on_disk_proposer

        except FileNotFoundError:
            self.run_id = 0

    def serialize(self):
        with open(FILE_NAME, "wb") as outfile:
            pickle.dump(self, outfile)

    def broadcast_prepare(self, propose_id: int) -> dict:
        mess = acceptor.PrepareMessage(run_id=self.run_id, propose_id=propose_id)
        responses = []

        logger.debug(f"Broadcasting PREPARE: {mess}")

        for id in range(1, NODES + 1):
            if id == NODE_ID:
                r = acceptor.acceptor_prepare(mess)
            else:
                try:
                    r = requests.put(f"http://node{id}:80/acceptor_prepare", json=mess.dict())
                    r = r.json()
                except Exception as error:
                    logger.debug(f"Sending PREPARE to node {id} failed. Reason: {error}")
                    continue
            responses.append(r)

        logger.debug(f"Received PROMISEs: {responses}")

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
            logger.debug(f"Majority of nodes did not respond to prepare message. Responses count: {len(responses)}")
            raise HTTPException(status_code=503, detail="Service unavailable.")
        return res

    def broadcast_accept(self, propose_id: int, val: bank.BankOperation) -> bool:
        mess: acceptor.AcceptMessage = acceptor.AcceptMessage(run_id=self.run_id, propose_id=propose_id, val=val)
        accepts_cnt = 0

        logger.debug(f"Broadcasting ACCEPT: {mess}")

        for id in range(1, NODES + 1):
            if id == NODE_ID:
                r = acceptor.acceptor_accept(mess)
            else:
                try:
                    r = requests.put(f"http://node{id}:80/acceptor_accept", json=mess.dict())
                    r = r.json()
                except Exception as error:
                    logger.debug(f"Sending ACCEPT to node {id} failed: {error}")
                    continue
            if r["accepted"]:
                accepts_cnt += 1

        logger.debug(f"{accepts_cnt} nodes accepted value {val} in paxos run {self.run_id}.")

        return accepts_cnt > NODES // 2

    def paxos(self, op: bank.BankOperation) -> bank.BankOperation:
        logger.debug(f"Proposing operation {op} in paxos run {self.run_id}")
        propose_id = NODE_ID
        retries = 0
        while True:
            res = self.broadcast_prepare(propose_id=propose_id)
            if "promised_id" in res:
                logger.debug(f"Received NACK response {res}.")
                propose_id = next_unique(res["promised_id"])
                backoff(retries)
                retries += 1
                continue
            elif res["accepted_val"] is None:
                logger.debug(f"Available nodes have not accepted any value for paxos run {self.run_id} yet.")
                # TODO validate op here
                pass
            else:
                logger.debug(f"Value accepted in paxos run {self.run_id} with highest accept_id: {res}. ")
                op = bank.BankOperation(**res["accepted_val"])

            accepted = self.broadcast_accept(propose_id=propose_id, val=op)
            if accepted:
                logger.debug(f"Operation {op} was accepted by majority of nodes in paxos run {self.run_id}.")
                return op
            else:
                logger.debug(f"Operation {op} was NOT accepted by majority of nodes in paxos run {self.run_id}.")
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
