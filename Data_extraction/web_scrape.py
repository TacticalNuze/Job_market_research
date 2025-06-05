import os
import time

from celery_app.tasks import web_scrape
from data_extraction.Websites import init_driver

print(f"The driver should be patched and put in {os.environ.get('CHROME_DRIVER_DIR')}")
print(f"The chrome binary should be patched and put in {os.environ.get('CHROME_BIN')}")

patch_driver = init_driver()
time.sleep(15)
patch_driver.quit()


web_scrape.delay()
