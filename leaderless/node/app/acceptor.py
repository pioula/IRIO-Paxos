class Acceptor:
    def __init__(self):
        self.promised_id = -1
        self.accepted_id = None
        self.accepted_val = None

    def handle_prepare(self, propose_id):
        if self.promised_id >= propose_id:
            self.promised_id = propose_id
            return self.accepted_id, self.accepted_val
        return None

    def handle_accept(self, propose_id, val):
        if propose_id >= self.promised_id:
            self.promised_id = propose_id
            self.accepted_id = propose_id
            self.accepted_val = val
            return True
        return False
