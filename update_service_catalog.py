import sys
import json
import logging
import logging.config
import socket
import utility
from service_cataloger import ServiceCataloger


def main(script_config):

    logger.info("Starting update process.")

    root_url = script_config['server']['admin_url']

    logger.info("Building service cataloger.")
    cataloger = ServiceCataloger(root_url)

    logger.info("Updating service catalog.")
    cataloger.update_catalog()

    logger.info("Update process complete.")


if __name__ == '__main__':

    try:

        with open('config.json', 'r') as f:
            main_config = json.load(f)

        with open('update_service_catalog.json', 'r') as f:
            script_config = json.load(f)

        logging.config.dictConfig(main_config['logging'])
        logger = logging.getLogger('UpdateServiceCatalog')
        logger.info("=========================")
        logger.info("Start logging.")

    except Exception as e:

        subject = "UpdateServiceCatalog - ERROR"
        result = "An error occurred while configuring the process."
        message = "Result: {0}\nHost: {1}".format(result, socket.gethostname())
        utility.send_message(subject, message)

    try:

        main(script_config)

        # subject = "SyncGisServices - SUCCESS"
        # result = "Successfully synced services."
        # message = "Result: {0}\nHost: {0}".format(result, socket.gethostname())
        # Utility.send_message(subject, message)

    except Exception as e:

        logger.exception("Fatal error in main process.")
        subject = "RetrieveGisServices - ERROR"
        result = "An error occurred while retrieving services."
        log_path = main_config['logging']['handlers']['fileHandler']['filename']
        message = "Result: {0}\nLog: {1}\nHost: {2}".format(result, log_path, socket.gethostname())
        utility.send_message(subject, message)

sys.exit()
