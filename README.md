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

## Execution

1. Client Node

```bash
docker run -it -p 8000:8000 chengyentsai851010/client
```

2. Master Node

```bash
docker run -it -p 8001:8001 chengyentsai851010/master_server
```

3. Update Node

```bash
docker run -it -p 8002:8002 chengyentsai851010/update_server
```
