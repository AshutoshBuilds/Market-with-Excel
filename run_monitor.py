"""Run Excel monitor."""
import logging
from .monitor import ExcelMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def run_monitor():
    """Run Excel monitor."""
    monitor = ExcelMonitor()
    monitor.start()

if __name__ == "__main__":
    run_monitor() 