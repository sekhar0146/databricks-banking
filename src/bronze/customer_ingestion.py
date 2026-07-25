from src.common.config import load_config
from src.common.logger import get_logger


def main():

    config = load_config()

    logger = get_logger(__name__)

    catalog = config["catalog"]
    bronze_schema = config["schemas"]["bronze"]
    volume_path = config["volumes"]["landing"]
    customer_table = config["tables"]["customer"]

    logger.info(f"Catalog       : {catalog}")
    logger.info(f"Bronze Schema : {bronze_schema}")
    logger.info(f"Volume        : {volume_path}")
    logger.info(f"Table         : {customer_table}")

    logger.info("Customer ingestion started")


if __name__ == "__main__":
    main()