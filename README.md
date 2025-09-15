# qdrant-chroma
Installation, configuration and benchmarks for qdrant and chroma vector databases

### Installation

```
python3 -m venv venv
```
```
. venv/bin/activate
```
```
pip install --no-cache-dir -r requirements.txt
```

### Measure performance of insertion and search operations
- Edit config.yaml under scripts directory and run tests:

```
# load data

python3 run_tests.py load
```
```
# search data

python3 run_tests.py search
```

Results are stored under out directory
