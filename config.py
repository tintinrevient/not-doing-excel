import yaml


def load(file_name: str):
    with open(file_name) as f:
        config_map = yaml.safe_load(f)

    return config_map


if __name__ == '__main__':
    config_map = load('config/config.yml')
    print(config_map)