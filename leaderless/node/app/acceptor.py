import pickle
import logging
from collections import defaultdict
from fastapi import APIRouter
from pydantic import BaseModel

import app.bank as bank

FILE_NAME = "acceptor.pickle"

logging.basicConfig()
logger = logging.getLogger("ACCEPTOR")
logger.setLevel(logging.DEBUG)


def create_default_params():
    return {"promised_id": -1, "accepted_id": -1, "accepted_val": None}


class Acceptor:
    def __init__(self):
        """
        Instantiates an Acceptor object from the on-disk file iff there is one available.
        """
        try:
            with open(FILE_NAME, "rb") as infile:
                on_disk_acceptor = pickle.load(infile)
                self.parameters = on_disk_acceptor.parameters
                logger.debug('Instantiated Acceptor object from a file.')

        except FileNotFoundError:
            self.parameters = defaultdict(create_default_params)
            logger.debug('No Acceptor object found on the disk! Creating a new one.')

    def handle_prepare(self, run_id, propose_id):
        run_parameters = self.parameters[run_id]

        if run_parameters["promised_id"] < propose_id:
            logger.debug(f'New propose_id ({propose_id}) for paxos run ({run_id}) higher than all promised before in '
                         f'paxos run ({run_id}).')

            run_parameters["promised_id"] = propose_id
            logger.debug(f'Promised to ignore propose_id < {run_parameters["promised_id"]}')
            return run_parameters["accepted_id"], run_parameters["accepted_val"]

        logger.debug(
            f'Already promised to ignore propose_id: ({propose_id}) <= promised_id: ({run_parameters["promised_id"]}) '
            f'in paxos run ({run_id}).')
        return None, run_parameters["promised_id"]

    def handle_accept(self, run_id, propose_id, val):
        run_parameters = self.parameters[run_id]

        if run_parameters["promised_id"] <= propose_id:
            logger.debug(f'New propose_id ({propose_id}) >= max promised_id ({run_parameters["promised_id"]}) for '
                         f'paxos run ({run_id}).')

            run_parameters["accepted_id"] = propose_id
            run_parameters["accepted_val"] = val
            logger.debug(f'Accepted value {val} with propose id {propose_id} for paxos run {run_id}.')
            return True

        logger.debug(
            f'Already promised to ignore promised_id: ({run_parameters["promised_id"]}) '
            f'< max_promised_id: ({run_parameters["promised_id"]}) in paxos run ({run_id}).')
        return False

    def serialize(self):
        with open(FILE_NAME, "wb") as outfile:
            pickle.dump(self, outfile)


class PrepareMessage(BaseModel):
    run_id: int = None
    propose_id: int = None

    def __init__(self, run_id: int, propose_id: int):
        super().__init__()
        self.run_id = run_id
        self.propose_id = propose_id

    def __str__(self) -> str:
        return self.__repr__()


class AcceptMessage(BaseModel):
    run_id: int = None
    propose_id: int = None
    val: bank.BankOperation = None

    def __init__(self, run_id: int, propose_id: int, val: bank.BankOperation):
        super().__init__()
        self.run_id = run_id
        self.propose_id = propose_id
        self.val = val

    def __str__(self) -> str:
        return self.__repr__()


router = APIRouter()
instance = Acceptor()


@router.put("/acceptor_prepare")
def acceptor_prepare(body: PrepareMessage):
    logger.debug(f"Received message {body}")
    res = instance.handle_prepare(body.run_id, body.propose_id)
    instance.serialize()
    if res[0] is not None:
        # Prepare operation succeeded!
        return {"accepted_id": res[0], "accepted_val": res[1]}
    # Prepare operation did not succeed, send NACK response.
    return {"promised_id": res[1]}


@router.put("/acceptor_accept")
def acceptor_accept(body: AcceptMessage):
    logger.debug(f"Received message {body}")
    res = instance.handle_accept(body.run_id, body.propose_id, body.val)
    instance.serialize()
    return {"accepted": res}
