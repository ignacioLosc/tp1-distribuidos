import yaml
import json
import argparse

NETWORK_NAME='steam_analyzer_net'
DEFAULT_LOG_LEVEL='INFO'
DOCKER_COMPOSE_FILENAME='docker-compose.yaml'

def basic_service(worker, env: list = [], container_name=None, log_level=DEFAULT_LOG_LEVEL):
    obj = {
        'build': {
            'context': '.',
            'dockerfile': './system/Dockerfile',
            'args': {
                'WORKER': worker
            }
        },
        'entrypoint': f'/app/{worker}',
        'environment': [f'CLI_LOG_LEVEL={log_level}'] + env,
        # 'networks': [NETWORK_NAME]
    }

    if container_name:
        obj['container_name'] = container_name
    else:
        obj['container_name'] = worker

    obj['depends_on'] = ['rabbitmq']
    if worker != 'gateway':
        obj['depends_on'].append('gateway')

    return obj


def generate_docker_compose(worker_list, log_level=DEFAULT_LOG_LEVEL, client_volumes=None, num_joiners=5, num_sorters=5, num_mappers=1, num_counters=1, batch_size=5):
    services = {}
    for worker in worker_list:
        services[worker] = basic_service(worker, log_level=log_level)

    for genre in ['Shooter', 'Indie']:
        for i in range(num_joiners):
            joiner_name = f'joiner{i}-{genre.lower()}'
            services[joiner_name] = basic_service('joiner', [f'CLI_JOINER_ID={i}', f'CLI_JOINER_GENRE={genre}', f'CLI_SORTERS={num_sorters}'], joiner_name, log_level=log_level)

    for i in range(num_sorters):
        name = f'positive-sorter-{i}'
        services[name] = basic_service('positive_sorter_top_5', [f'CLI_SORTER_ID={i}', f'CLI_TOP=5', f'CLI_JOINERS={num_joiners}'], name, log_level=log_level)

    for i in range(num_mappers):
        name = f'review-mapper-{i}'
        services[name] = basic_service('review_mapper', [f'CLI_MAPPER_ID={i}', f'CLI_JOINERS={num_joiners}'], name, log_level=log_level)

    for i in range(num_counters):
        name = f'platform-counter-{i}'
        services[name] = basic_service('platform_counter', [f'CLI_COUNTER_ID={i}'], name, log_level=log_level)

    services['gateway']['environment'].append('CLI_SERVER_PORT=5555')
    services['gateway']['environment'].append(f'CLI_MAPPERS={num_mappers}')

    services['client']['volumes'] = client_volumes if client_volumes else [
        './.data/games.csv:/app/games.csv',
        './.data/dataset.csv:/app/dataset.csv'
    ]
    services['client']['environment'].append('CLI_SERVER_ADDRESS=gateway:5555')
    services['client']['environment'].append('CLI_DATA_PATH=.data')
    services['client']['environment'].append(f'CLI_BATCH_SIZE={batch_size}')

    services['5k_reviews_aggregator']['environment'].append(f'CLI_JOINERS={num_joiners}')
    services['90th_percentile_calculator']['environment'].append(f'CLI_JOINERS={num_sorters}')
    services['top_5_aggregator']['environment'].append(f'CLI_SORTERS={num_sorters}')

    services['genre_filter']['environment'].append(f'CLI_JOINERS={num_joiners}')

    services['platform_accumulator']['environment'].append(f'CLI_COUNTERS={num_counters}')
    services['gateway']['environment'].append(f'CLI_COUNTERS={num_counters}')

    services['rabbitmq'] = {
            'image': 'rabbitmq:3.9.16-management-alpine',
            'container_name': 'rabbitmq',
            'ports': ['5672:5672', '15672:15672'],
            'logging': {
                'driver': 'none'
            },
            'environment': {
                'RABBITMQ_DEFAULT_USER': 'guest',
                'RABBITMQ_DEFAULT_PASS': 'guest'
            },
            # 'networks': [NETWORK_NAME],
    }


    compose_data = {
        'name': 'steam-analyzer',
        'services': services,
        'networks': {
            NETWORK_NAME: {
                # 'external': True,
                # 'name': NETWORK_NAME
            }
        }
    }

    with open(DOCKER_COMPOSE_FILENAME, 'w') as file:
        yaml.dump(compose_data, file, default_flow_style=False)

def open_config(filename='config.json'):
    try:
        with open(filename) as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"File {filename} not found.")
        exit(1)
    except json.JSONDecodeError:
        print(f"File {filename} is not a valid JSON file.")
        exit(1)
    except Exception as e:
        print(f"An error occurred while opening the file {filename}: {e}")
        exit(1)

parser = argparse.ArgumentParser(description='Load a JSON config file.')
parser.add_argument(
    '--config_file',
    type=str,
    default='config.json',
    help='Path to the JSON config file (optional)'
)

args = parser.parse_args()
config = open_config(args.config_file)


worker_list = [
    "client",
    "gateway",
    "90th_percentile_calculator",
    "5k_reviews_aggregator",
    "genre_filter",
    "platform_accumulator",
    "time_played_sorter",
    "top_5_aggregator"

    # "platform_counter",
    # "review_mapper",
    # "positive_sorter_top_5",
    # "joiner"
]

game_file = config['GAMES_FILE']
if not game_file:
    print("Please provide the games file path ('GAMES_FILE') in the config ")
    exit(1)

reviews_file = config['REVIEWS_FILE']
if not game_file:
    print("Please provide the reviews file path ('REVIEWS_FILE') in the config")
    exit(1)

client_volumes = [f'{game_file}:/app/games.csv', f'{reviews_file}:/app/dataset.csv']
generate_docker_compose(worker_list,
                        log_level=config.get("LOG_LEVEL", DEFAULT_LOG_LEVEL),
                        client_volumes=client_volumes,
                        num_joiners=config.get("NUM_JOINERS", None),
                        num_sorters=config.get("NUM_SORTERS", None),
                        num_mappers=config.get("NUM_MAPPERS", None),
                        num_counters=config.get("NUM_COUNTERS", None),
                        batch_size=config.get("BATCH_SIZE", None)
                        )
