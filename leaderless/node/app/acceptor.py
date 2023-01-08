import pickle

FILE_NAME = "acceptor.pickle"


class Acceptor:
    def __init__(self):
        """
        Instantiates an Acceptor object from the on-disk file iff there is one available.
        """
        try:
            with open(FILE_NAME, "rb") as infile:
                on_disk_acceptor = pickle.load(infile)

                self.promised_id = on_disk_acceptor.promised_id
                self.accepted_id = on_disk_acceptor.accepted_id
                self.accepted_val = on_disk_acceptor.accepted_val

        except FileNotFoundError:
            self.promised_id = -1
            self.accepted_id = None
            self.accepted_val = None

    def handle_prepare(self, propose_id):
        if self.promised_id <= propose_id:
            self.promised_id = propose_id
            return self.accepted_id, self.accepted_val
        return None, self.promised_id

    def handle_accept(self, propose_id, val):
        if self.promised_id <= propose_id:
            self.accepted_id = propose_id
            self.accepted_val = val
            return True
        return False

    def serialize(self):
        with open(FILE_NAME, "wb") as outfile:
            pickle.dump(self, outfile)
