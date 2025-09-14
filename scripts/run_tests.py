import sys
import time
import yaml
import json
import numpy as np
from qdrant_client import QdrantClient, models
from tqdm import  tqdm

def main():
    with open('config.yaml', 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

        if sys.argv[1] == 'load':
            res = load_data(config.get('load_config', None))
            if res is None:
                sys.exit(1)
        elif sys.argv[1] == 'search':
            res = search_data(config.get('search_config',None))
            if res is None:
                sys.exit(1)
        else:
            print('Unknown option: Valid options are run_tests.py load or run_tests.py search')
            print('- Run load config tests   -- run_tests.py load')
            print('- Run search config tests -- run_tests.py search')
            sys.exit(1)


def load_data(config):
    if config is None:
        print('Load config not found')
        return None
    config_size = len(config)
    current_config = 1
    for item in config:
        print(f"=> Running config {current_config}/{config_size}")
        if item.get('db_type') == 'qdrant':
            client = QdrantClient(url=item.get('url'))

            # Default distance is Cosine
            client.create_collection(
                    collection_name=item.get('collection'),
                    vectors_config = models.VectorParams(size=item.get('dimension'), distance=models.Distance.COSINE))
            start_time = time.perf_counter()
            with open(item.get('path_to_payload'), 'r') as payloads_file:
                vectors = np.load(item.get('path_to_vectors'), mmap_mode='r')
                id = 1
                for payload in tqdm(payloads_file):
                    vector_payload = json.loads(payload)
                    client.upsert(
                            collection_name=item.get('collection'),
                            points=[models.PointStruct(id=id,vector=vectors[id-1], payload=vector_payload)],
                            )
                    id += 1
            end_time = time.perf_counter()
            with open(f"out/{item.get('collection')}/result.json", 'w') as output_file:
                _tmp = item
                _tmp['duration'] = end_time - start_time
                output_file.write(json.dumps(_tmp, indent=4))
        elif item.get('db_type') == 'chroma':
            pass
        else:
            print('Unknown type: object {}'.format(item))
        current_config += 1
        
    

def search_data(config):
    if config is None:
        print('Search config not found')
        return None


if __name__ == '__main__':
    main()
