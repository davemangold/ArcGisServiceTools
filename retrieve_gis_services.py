import sys
import json
import logging
import logging.config
import socket
import utility
from feature_layer import FeatureLayer
from feature_retriever import FeatureRetriever


def main(main_config, job_config, job_name):

    logger.info("Starting retrieval process.")

    logger.info("Starting retrieval job for: {0}.".format(job_name))
    src_config = job_config['source']
    src_url = src_config['url']
    src_token = utility.get_service_token(src_config)
    src_certificate = utility.get_service_certificate(src_config)

    logger.info("Creating source feature layer object from: {0}".format(src_url))
    fl_args = [src_url, src_token, src_certificate]
    src_fl = FeatureLayer(main_config, src_config, *fl_args)

    logger.info("Creating feature retriever object.")
    tgt_config = job_config['target']
    tgt_path = tgt_config['path']
    tgt_format = tgt_config['format']
    fr = FeatureRetriever(src_fl, tgt_path, job_name, tgt_format)

    logger.info("Retrieving features.")
    fr.retrieve()

    logger.info("Retrieval job complete for: {0}.".format(job_name))

    logger.info("Retrieval process complete.")


if __name__ == '__main__':

    job_name = sys.argv[1]

    try:

        with open('config.json', 'r') as f:
            main_config = json.load(f)

        with open('retrieve_gis_services.json', 'r') as f:
            script_config = json.load(f)

        job_config = script_config['jobs'][job_name]

        logging.config.dictConfig(main_config['logging'])
        logger = logging.getLogger('RetrieveGisServices')
        logger.info("=========================")
        logger.info("Start logging.")

    except Exception as e:

        subject = "RetrieveGisServices - ERROR"
        result = "An error occurred while configuring the process."
        message = "Result: {0}\nHost: {1}".format(result, socket.gethostname())
        utility.send_message(subject, message)

    try:

        main(main_config, job_config, job_name)

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
