import sys
import time
import yaml
import json
import datetime
import numpy as np
from qdrant_client import QdrantClient, models
import chromadb
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
        print(f"=> Running load config {current_config}/{config_size}")
        if item.get('db_type') == 'qdrant':
            client = QdrantClient(url=item.get('url'))

            # Default distance is Cosine
            client.create_collection(
                    collection_name=item.get('collection'),
                    vectors_config = models.VectorParams(size=item.get('dimension'), distance=models.Distance.COSINE))
            start_date = datetime.datetime.now()
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
            end_date = datetime.datetime.now()
            with open(f"out/{item.get('collection')}_{item.get('db_type')}_result.json", 'w') as output_file:
                _tmp = item
                _tmp['start_time'] = start_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['end_time'] = end_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['duration'] = end_time - start_time
                output_file.write(json.dumps(_tmp, indent=4))
        elif item.get('db_type') == 'chroma':
            chroma_client = chromadb.HttpClient(host=item.get('url'), port=8000)

            chroma_collection = chroma_client.get_or_create_collection(
                    name=item.get('collection'),
                    configuration={
                        "hnsw": {
                            "space": "cosine",
                            }
                        },
                    )

            start_date = datetime.datetime.now()
            start_time = time.perf_counter()

            with open(item.get('path_to_payload'), 'r') as payloads_file:
                vectors = np.load(item.get('path_to_vectors'), mmap_mode='r')
                
                batch_start = 0
                batch_end = 1000
                _payload_list = []
                for payload in tqdm(payloads_file):
                    vector_payload = json.loads(payload)
                    _payload_list.append(vector_payload)

                    if len(_payload_list) == 1000:
                        chroma_collection.add(
                                embeddings=vectors[batch_start:batch_end],
                                metadatas=_payload_list,
                                ids=[str(i) for i in rage(batch_start,batch_end)]
                                )
                        del _payload_list[:]
                        batch_start += 1000
                        batch_end += 1000

            end_time = time.perf_counter()
            end_date = datetime.datetime.now()
            with open(f"out/{item.get('collection')}_{item.get('db_type')}_result.json", 'w') as output_file:
                _tmp = item
                _tmp['start_time'] = start_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['end_time'] = end_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['duration'] = end_time - start_time
                output_file.write(json.dumps(_tmp, indent=4))
        else:
            print('Unknown type: object {}'.format(item))
        current_config += 1

def search_data(config):
    if config is None:
        print('Search config not found')
        return None
    config_size = len(config)
    current_config = 1
    for item in config:
        print(f"=> Running search config {current_config}/{config_size}")
        if item.get('db_type') == 'qdrant':
            client = QdrantClient(url=item.get('url'))

            start_date = datetime.datetime.now()
            start_time = time.perf_counter()
            with open(item.get('path_to_tests'), 'r') as tests_file:
                for test in tqdm(tests_file):
                    query = json.loads(test)
                    client.query_points(
                            collection_name=item.get('collection'),
                            query=query['query'],
                            )
            end_time = time.perf_counter()
            end_date = datetime.datetime.now()
            with open(f"out/search_{item.get('collection')}_{item.get('db_type')}_result.json", 'w') as output_file:
                _tmp = item
                _tmp['start_time'] = start_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['end_time'] = end_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['duration'] = end_time - start_time
                output_file.write(json.dumps(_tmp, indent=4))
        elif item.get('db_type') == 'chroma':
            chroma_client = chromadb.HttpClient(host=item.get('url'), port=8000)
            collection = chroma_client.get_collection(name=item.get('collection'))
            start_date = datetime.datetime.now()
            start_time = time.perf_counter()

            with open(item.get('path_to_tests'), 'r') as tests_file:
                for tests in tqdm(tests_file):
                    query = json.loads(test)
                    collection.query_embeddings=[query['query']]

            end_time = time.perf_counter()
            end_date = datetime.datetime.now()
            with open(f"out/search_{item.get('collection')}_{item.get('db_type')}_result.json", 'w') as output_file:
                _tmp = item
                _tmp['start_time'] = start_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['end_time'] = end_date.strftime("%Y-%m-%d %H:%M:%S")
                _tmp['duration'] = end_time - start_time
                output_file.write(json.dumps(_tmp, indent=4))
        else:
            print('Unknown type: object {}'.format(item))
        current_config += 1


if __name__ == '__main__':
    main()
