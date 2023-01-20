import os
import random
import time
import requests

from database import *

random.seed(10)

accounts = [{}, {}]
db = connect()

PATHS = ['deposit', 'withdraw', 'transfer', 'open', 'quit']
NODE_PORTS = range(8001, 8006)
DUMMY_PORT = 8006
LOCALHOST = 'http://localhost'

def wait_for_nodes_to_wake_up():
    for port in NODE_PORTS:
        while True:
            try:
                requests.get(f'{LOCALHOST}:{port}/health')
                break
            except Exception as error:
                print(f" Node {port % 10} is not ready yet..")
                time.sleep(1)

def exec_open(port):
    print(f"op=open node={port % 10}")
    start_date = time.time()
    status_code = 500
    try:
        r = requests.post(f'{LOCALHOST}:{port}/open')
        delay_sec = time.time() - start_date
        status_code = r.status_code
        print(r)
        if r.status_code == 200:
            r = r.json()
            accounts[1 if port == DUMMY_PORT else 0][r['id']] = r['balance']
        return [delay_sec, status_code]
    except Exception:
        delay_sec = time.time() - start_date
        print("Request failed.")
        return [delay_sec, status_code]

def exec_withdraw(port, id, amount):
    print(f"op=withdraw node={port % 10} account_id={id} amount={amount}")
    start_date = time.time()
    status_code = 500
    try:
        r = requests.put(f'{LOCALHOST}:{port}/withdraw', json={"id": id, "amount": amount})
        delay_sec = time.time() - start_date
        status_code = r.status_code
        print(r)
        if r.status_code == 200:
            accounts[1 if port == DUMMY_PORT else 0][id] -= amount
        return [delay_sec, status_code]
    except Exception:
        delay_sec = time.time() - start_date
        print("Request failed.")
        return [delay_sec, status_code]

def exec_deposit(port, id, amount):
    print(f"op=deposit node={port % 10} account_id={id} amount={amount}")
    start_date = time.time()
    status_code = 500
    try:
        r = requests.put(f'{LOCALHOST}:{port}/deposit', json={"id": id, "amount": amount})
        delay_sec = time.time() - start_date
        status_code = r.status_code
        print(r)
        if r.status_code == 200:
            accounts[1 if port == DUMMY_PORT else 0][id] += amount
        return [delay_sec, status_code]
    except Exception:
        delay_sec = time.time() - start_date
        print("Request failed.")
        return [delay_sec, status_code]

def exec_transfer(port, id1, id2, amount):
    print(f"op=transfer node={port % 10} from_id={id1} to_id={id2} amount={amount}")
    start_date = time.time()
    status_code = 500
    try:
        r = requests.put(f'{LOCALHOST}:{port}/transfer', json={"from_id": id1, "to_id": id2, "amount": amount})
        delay_sec = time.time() - start_date
        status_code = r.status_code
        print(r)
        if r.status_code == 200:
            accounts[1 if port == DUMMY_PORT else 0][id1] -= amount
            accounts[1 if port == DUMMY_PORT else 0][id2] += amount
        return [delay_sec, status_code]
    except Exception:
        delay_sec = time.time() - start_date
        print("Request failed.")
        return [delay_sec, status_code]

def exec_quit(port):
    print(f"op=quit node={port % 10}")
    start_date = time.time()
    status_code = 500
    try:
        requests.get(f'{LOCALHOST}:{port}/quit')
        delay_sec = time.time() - start_date
        status_code = r.status_code
        return [delay_sec, status_code]
    except Exception:
        delay_sec = time.time() - start_date
        print("Request failed.")
        return [delay_sec, status_code]

def add_new_request(request_id, port, path, result):
    global db
    with db.cursor() as curr:
        write_query(curr, f"INSERT INTO requests (request_id, node_id, request_type, delay_sec, status_code) \
                    VALUES ({request_id}, {port % 10}, \'{path}\', {result[0]}, {result[1]})")
        db.commit()

def random_request(request_id):
    path = random.choice(PATHS)
    port = random.choice(NODE_PORTS)
    match path:
        case 'open':
            add_new_request(request_id, port, path, exec_open(port))
            add_new_request(request_id, DUMMY_PORT, path, exec_open(DUMMY_PORT))
            return
        case 'deposit':
            id1 = random.choice(list(accounts[0].keys()))
            id2 = random.choice(list(accounts[1].keys()))
            amount = random.randint(1, 100)
            add_new_request(request_id, port, path, exec_deposit(port, id1, amount))
            add_new_request(request_id, DUMMY_PORT, path, exec_deposit(DUMMY_PORT, id2, amount))
            return
        case 'withdraw':
            id1 = random.choice(list(accounts[0].keys()))
            id2 = random.choice(list(accounts[1].keys()))
            amount = random.randint(1, 100)
            add_new_request(request_id, port, path, exec_withdraw(port, id1, amount))
            add_new_request(request_id, DUMMY_PORT, path, exec_withdraw(DUMMY_PORT, id2, amount))
            return
        case 'transfer':
            account_keys = list(accounts[0].keys())
            id1 = random.choice(account_keys)
            account_keys.remove(id1)
            id2 = random.choice(account_keys)
            amount = random.randint(1, 20)
            add_new_request(request_id, port, path, exec_transfer(port, id1, id2, amount))
            account_keys = list(accounts[1].keys())
            id1 = random.choice(account_keys)
            account_keys.remove(id1)
            id2 = random.choice(account_keys)
            add_new_request(request_id, DUMMY_PORT, path, exec_transfer(DUMMY_PORT, id1, id2, amount))
            return
        case 'quit':
            add_new_request(request_id, port, path, exec_quit(port))
            id = random.choice(list(accounts[1].keys()))
            add_new_request(request_id, DUMMY_PORT, path, exec_deposit(DUMMY_PORT, id, 0)) # Placeholder for quit request
            return

def get_state(node_port):
    node_state = {}
    for id in accounts[0].keys():
        print(f"Reading balance of account {id}.")
        r = requests.put(f'{LOCALHOST}:{node_port}/deposit', json={"id": id, "amount": 0})
        print(r)
        if r.status_code == 200:
            node_state[id] = r.json()["balance"]
        print(node_state)
    return node_state


os.system("docker compose down >/dev/null 2>&1")
os.system("docker compose up --build &")

wait_for_nodes_to_wake_up()

# For tests
r = requests.post(f'{LOCALHOST}:8001/open').json()
accounts[0][r['id']] = r['balance']
r = requests.post(f'{LOCALHOST}:8001/open').json()
accounts[0][r['id']] = r['balance']

# For dummy
r = requests.post(f'{LOCALHOST}:8006/open').json()
accounts[1][r['id']] = r['balance']
r = requests.post(f'{LOCALHOST}:8006/open').json()
accounts[1][r['id']] = r['balance']

print("Sending random requests..")
for i in range(2000):
    print(i)
    random_request(i)

time.sleep(2)
print("Waiting for all nodes to wake up..")
wait_for_nodes_to_wake_up()
print("Reading node states..")



states = {}
for port in NODE_PORTS:
    print(f"Reading state of node {port % 10}")
    s = get_state(port)
    states[port] = s
    print(states[port])

print(f"Expected node state: {accounts[0]}")

for node_port in NODE_PORTS:
    print(f"Actual node {node_port % 10} state: {states[node_port]}")

for node_port in NODE_PORTS:
    assert states[node_port] == accounts[0]
