import yaml
from pathlib import Path


def load_config():

    #project_root = Path(__file__).parents[2]
    project_root = Path(__file__).resolve().parents[2]

    config_file = project_root / "configs" / "dev.yml"

    with open(config_file, "r") as file:
        config = yaml.safe_load(file)

    return config