import os
import random
from time import sleep

import requests

random.seed(10)

accounts = {}

PATHS = ['open', 'deposit', 'withdraw', 'transfer', 'quit']
NODE_PORTS = range(8001, 8006)
LOCALHOST = 'http://localhost'

def wait_for_nodes_to_wake_up():
    for port in NODE_PORTS:
        while True:
            try:
                requests.get(f'{LOCALHOST}:{port}/health')
                break
            except Exception as error:
                print(f" Node {port % 10} is not ready yet..")
                sleep(1)

def exec_open(port):
    print(f"op=open node={port % 10}")
    r = requests.post(f'{LOCALHOST}:{port}/open')
    print(r)
    if r.status_code == 200:
        r = r.json()
        accounts[r['id']] = r['balance']
    return

def exec_withdraw(port, id, amount):
    print(f"op=withdraw node={port % 10} account_id={id} amount={amount}")
    r = requests.put(f'{LOCALHOST}:{port}/withdraw', json={"id": id, "amount": amount})
    print(r)
    if r.status_code == 200:
        accounts[id] -= amount
    return

def exec_deposit(port, id, amount):
    print(f"op=deposit node={port % 10} account_id={id} amount={amount}")
    r = requests.put(f'{LOCALHOST}:{port}/deposit', json={"id": id, "amount": amount})
    print(r)
    if r.status_code == 200:
        accounts[id] += amount
    return

def exec_transfer(port, id1, id2, amount):
    print(f"op=transfer node={port % 10} from_id={id1} to_id={id2} amount={amount}")
    r = requests.put(f'{LOCALHOST}:{port}/transfer', json={"from_id": id1, "to_id": id2, "amount": amount})
    print(r)
    if r.status_code == 200:
        accounts[id1] -= amount
        accounts[id2] += amount
    return

def exec_quit(port):
    print(f"op=quit node={port % 10}")
    requests.get(f'{LOCALHOST}:{port}/quit')

def random_request():
    try:
        path = random.choice(PATHS)
        port = random.choice(NODE_PORTS)
        match path:
            case 'open':
                return exec_open(port)
            case 'deposit':
                id = random.choice(list(accounts.keys()))
                amount = random.randint(1, 100)
                return exec_deposit(port, id, amount)
            case 'withdraw':
                id = random.choice(list(accounts.keys()))
                amount = random.randint(1, 100)
                return exec_withdraw(port, id, amount)
            case 'transfer':
                account_keys = list(accounts.keys())
                id1 = random.choice(account_keys)
                account_keys.remove(id1)
                id2 = random.choice(account_keys)
                amount = random.randint(1, 20)
                return exec_transfer(port, id1, id2, amount)
            case 'quit':
                return exec_quit(port)
        return
    except Exception:
        pass

def get_state(node_port):
    node_state = {}
    for id in accounts.keys():
        print(f"Reading balance of account {id}.")
        r = requests.put(f'{LOCALHOST}:{node_port}/deposit', json={"id": id, "amount": 0})
        if r.status_code == 200:
            node_state[id] = r.json()["balance"]
    return node_state


os.system("docker compose down >/dev/null 2>&1")
os.system("docker compose up >/dev/null 2>&1 &")

wait_for_nodes_to_wake_up()

r = requests.post(f'{LOCALHOST}:8001/open')
r = requests.post(f'{LOCALHOST}:8001/open')
accounts['0'] = 0
accounts['1'] = 0

print("Sending random requests..")
for i in range(1000):
    random_request()

print("Waiting for all nodes to wake up..")
wait_for_nodes_to_wake_up()
print("Reading node states..")

states = {}
for port in NODE_PORTS:
    print(f"Reading state of node {port % 10}")
    states[port] = get_state(port)

print(f"Expected node state: {accounts}")

for node_port in states.keys():
    print(f"Actual node {node_port % 10} state: {states[node_port]}")

for node_port in states.keys():
    assert states[node_port] == accounts
