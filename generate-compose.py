import yaml

NETWORK_NAME='testing_net_tp1'
DEFAULT_LOG_LEVEL='INFO'
DOCKER_COMPOSE_FILENAME='docker-compose.yaml'

def basic_service(worker, env: list = [], container_name=None):
    obj = {
        'build': {
            'context': '.',
            'dockerfile': './system/Dockerfile',
            'args': {
                'WORKER': worker
            }
        },
        'entrypoint': f'/app/{worker}',
        'environment': [f'CLI_LOG_LEVEL={DEFAULT_LOG_LEVEL}'] + env,
        'networks': [NETWORK_NAME]
    }

    if container_name:
        obj['container_name'] = container_name
    else:
        obj['container_name'] = worker

    if worker != 'gateway':
        obj['depends_on'] = ['gateway']

    return obj


def generate_docker_compose(worker_list, client_volumes=None, num_joiners=5, num_sorters=5):
    services = {}
    for worker in worker_list:
        services[worker] = basic_service(worker) 

    services['gateway']['environment'].append('CLI_SERVER_PORT=5555')

    services['client']['volumes'] = client_volumes if client_volumes else [
        './.data/games.csv:/app/games.csv',
        './.data/dataset.csv:/app/dataset.csv'
    ]
    services['client']['environment'].append('CLI_SERVER_ADDRESS=gateway:5555')
    services['client']['environment'].append('CLI_DATA_PATH=.data')

    services['5k_reviews_aggregator']['environment'].append(f'CLI_SORTERS={num_sorters}')
    services['90th_percentile_calculator']['environment'].append(f'CLI_SORTERS={num_sorters}')

    for genre in ['Shooter', 'Indie']:
        for i in range(num_joiners):
            joiner_name = f'joiner{i}-{genre.lower()}'
            services[joiner_name] = basic_service('joiner', [f'CLI_JOINER_ID={i}', f'CLI_JOINER_GENRE={genre}'], joiner_name)

    for i in range(num_joiners):
        sorter_name = f'positive-sorter-{i}'
        services[sorter_name] = basic_service('positive_sorter_top_5', [f'CLI_SORTER_ID={i}'], sorter_name)

    compose_data = {
        'name': 'steam-analyzer',
        'services': services,
        'networks': {
            'testing_net_tp1': {
                'external': True,
                'name': 'testing_net_tp1'
            }
        }
    }

    with open(DOCKER_COMPOSE_FILENAME, 'w') as file:
        yaml.dump(compose_data, file, default_flow_style=False)


# Example usage
worker_list = [
    "client",
    "gateway",
    "90th_percentile_calculator",
    "5k_reviews_aggregator",
    "genre_filter",
    "platform_accumulator",
    "platform_counter",
    "review_mapper",
    "time_played_sorter",
    "top_5_aggregator"

    # "positive_sorter_top_5",
    # "joiner"
]

client_volumes = ['./data/games.csv:/app/games.csv', './data/dataset.csv:/app/dataset.csv']
generate_docker_compose(worker_list, client_volumes, num_joiners=5, num_sorters=5)
