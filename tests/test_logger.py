from src.common.logger import get_logger

logger = get_logger(__name__)

logger.info("Customer ingestion started")
logger.warning("Sample warning")
logger.error("Sample error")