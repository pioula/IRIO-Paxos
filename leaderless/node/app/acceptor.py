import pickle
import logging
from collections import defaultdict

FILE_NAME = "acceptor.pickle"

logger = logging.getLogger()
c_handler = logging.StreamHandler()
logger.addHandler(c_handler)
logger.setLevel(logging.DEBUG)


def create_default_params():
    return {"promised_id": -1, "accepted_id": None, "accepted_val": None}


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
            logger.debug('New propose_id higher than all promised before.')

            run_parameters["promised_id"] = propose_id
            return run_parameters["accepted_id"], run_parameters["accepted_val"]

        logger.debug(f'Already promised not smaller ({run_parameters["promised_id"]}) proposal than current propose_id.')
        return None, run_parameters["promised_id"]

    def handle_accept(self, run_id, propose_id, val):
        run_parameters = self.parameters[run_id]

        if run_parameters["promised_id"] < propose_id:
            logger.debug('New propose_id higher than all promised before.')

            run_parameters["accepted_id"] = propose_id
            run_parameters["accepted_val"] = val
            return True

        logger.debug(f'Already promised not smaller ({run_parameters["promised_id"]}) proposal than current propose_id.')
        return False

    def serialize(self):
        with open(FILE_NAME, "wb") as outfile:
            pickle.dump(self, outfile)
