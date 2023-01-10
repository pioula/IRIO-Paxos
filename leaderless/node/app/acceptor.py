import pickle
from collections import defaultdict

FILE_NAME = "acceptor.pickle"


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

        except FileNotFoundError:
            self.parameters = defaultdict(create_default_params)

    def handle_prepare(self, run_id, propose_id):
        run_parameters = self.parameters[run_id]

        if run_parameters["promised_id"] < propose_id:
            run_parameters["promised_id"] = propose_id
            return run_parameters["accepted_id"], run_parameters["accepted_val"]
        return None, run_parameters["promised_id"]

    def handle_accept(self, run_id, propose_id, val):
        run_parameters = self.parameters[run_id]

        if run_parameters["promised_id"] < propose_id:
            run_parameters["accepted_id"] = propose_id
            run_parameters["accepted_val"] = val
            return True
        return False

    def serialize(self):
        with open(FILE_NAME, "wb") as outfile:
            pickle.dump(self, outfile)
