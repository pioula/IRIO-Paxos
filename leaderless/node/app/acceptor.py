import pickle
import logging
from collections import defaultdict
from fastapi import APIRouter
from pydantic import BaseModel

import app.bank as bank

FILE_NAME = "acceptor.pickle"

logger = logging.getLogger('ACCEPTOR')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('acceptor.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:    %(name)s:    %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


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

    def log(self, run_id: int, propose_id: int, state: dict, message: str):
        logger.debug(f"[RUN: {run_id}] [PROPOSE_ID: {propose_id}] [ACCEPTOR_STATE: {state}]:    {message}")

    def handle_prepare(self, run_id, propose_id):
        run_parameters = self.parameters[run_id]

        if run_parameters["promised_id"] < propose_id:
            self.log(run_id, propose_id, run_parameters, f'Promised to ignore propose id <= {propose_id}.')

            run_parameters["promised_id"] = propose_id
            return run_parameters["accepted_id"], run_parameters["accepted_val"]

        self.log(run_id, propose_id, run_parameters, f'Ignoring request.')
        return None, run_parameters["promised_id"]

    def handle_accept(self, run_id, propose_id, val):
        run_parameters = self.parameters[run_id]

        if run_parameters["promised_id"] <= propose_id:
            run_parameters["accepted_id"] = propose_id
            run_parameters["accepted_val"] = val
            self.log(run_id, propose_id, run_parameters, f'Accepted value {val}.')
            return True

        self.log(run_id, propose_id, run_parameters, f'Ignoring request.')
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
    logger.debug(f"Received {body}")
    res = instance.handle_prepare(body.run_id, body.propose_id)
    instance.serialize()
    if res[0] is not None:
        # Prepare operation succeeded!
        return {"accepted_id": res[0], "accepted_val": res[1]}
    # Prepare operation did not succeed, send NACK response.
    return {"promised_id": res[1]}


@router.put("/acceptor_accept")
def acceptor_accept(body: AcceptMessage):
    logger.debug(f"Received {body}")
    res = instance.handle_accept(body.run_id, body.propose_id, body.val)
    instance.serialize()
    return {"accepted": res}
