from src.common.config import load_config

config = load_config("configs/dev.yml")

print(config)
print(config["catalog"])
print(config["schemas"]["bronze"])
print(config["volumes"]["landing"])