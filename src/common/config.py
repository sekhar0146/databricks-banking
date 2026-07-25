import yaml


def load_config(config_file):
    """
    Read YAML configuration file.
    """

    with open(config_file, "r") as file:
        config = yaml.safe_load(file)

    return config