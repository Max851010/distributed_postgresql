# distributed_postgresql

## Environments

1. use virtualenv

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
pip install -r requirements.txt
```

2. deactivate virtualenv

```bash
deactivate
```

3. Update requirements.txt

```bash
pip3 freeze > requirements.txt
```

## Execution locally

1. Download the repository

```bash
git clone https://github.com/Max851010/distributed_postgresql.git
```

2. Download PostgreSQL locally

- [PostgreSQL](https://www.postgresql.org/download/)
- create user `kenyang` with password `ken890404`

3. Prepare 6 nodes

- Client node
  Note you have to modify all the port and address in `db_client.py`
  ```bash
  cd client
  pip install -r requirements.txt
  python3 db_client.py
  ```
- Master node
  Note you have to modify all the port and address in `master_server.py`
  ```bash
  cd master_server
  pip install -r requirements.txt
  python3 master_server.py
  ```
- Update node
  Note you have to modify all the port and address in `update_server.py`
  ```bash
  cd update_server
  pip install -r requirements.txt
  cd update
  python3 update_server.py
  ```
- Replica node
  Note you have to modify all the port and address in `replica_server.py`
  ```bash
  cd update_server
  pip install -r requirements.txt
  cd replica
  python3 replica_server.py
  ```
