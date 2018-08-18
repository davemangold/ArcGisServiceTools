import sys
import json
import logging
import logging.config
import socket
import utility
from feature_layer import FeatureLayer
from feature_syncer import FeatureSyncer
from attribute_mapper import AttributeMapper
from feature_mailer import FeatureMailer


def main(main_config, job_config, job_name):

    logger.info("Starting sync process.")

    logger.info("Starting sync job for: {0}.".format(job_name))

    logger.info("Creating attribute mapper object.")
    am = AttributeMapper(job_config['attribute_map'])

    src_config = job_config['source']
    src_url = src_config['url']
    src_token = utility.get_service_token(src_config)
    src_certificate = utility.get_service_certificate(src_config)

    logger.info("Creating source feature layer object from: {0}".format(src_config['url']))
    src_fl_args = [src_url, src_token, src_certificate]
    src_fl = FeatureLayer(main_config, src_config, *src_fl_args)

    tgt_config = job_config['target']
    tgt_url = tgt_config['url']
    tgt_token = utility.get_service_token(tgt_config)
    tgt_certificate = utility.get_service_certificate(tgt_config)

    logger.info("Creating target feature layer object from: {0}".format(tgt_config['url']))
    tgt_fl_args = [tgt_url, tgt_token, tgt_certificate]
    tgt_fl = FeatureLayer(main_config, tgt_config, *tgt_fl_args)

    logger.info("Creating feature syncer object.")
    fs = FeatureSyncer(src_fl, tgt_fl, am)

    logger.info("Syncing features ({0}).".format(job_config['type']))
    fs.sync(src_config['uid'], tgt_config['uid'], job_config['type'], job_config['favor'])

    if job_config['mailer']['send_mail'] is True:

        logger.info("Creating feature mailer object.")
        fm = FeatureMailer(main_config, job_config['mailer'], fs)

        logger.info("Mailing feature reports.")
        fm.mail_features()

    logger.info("Sync job complete for {0}.".format(job_name))

    logger.info("Sync process complete.")


if __name__ == '__main__':

    job_name = sys.argv[1]

    try:

        with open('config.json', 'r') as f:
            main_config = json.load(f)

        with open('sync_gis_services.json', 'r') as f:
            script_config = json.load(f)

        job_config = script_config['jobs'][job_name]

        logging.config.dictConfig(main_config['logging'])
        logger = logging.getLogger('SyncGisServices')
        logger.info("=========================")
        logger.info("Start logging.")

    except Exception as e:

        subject = "SyncGisServices - ERROR"
        result = "An error occurred while configuring the process."
        message = "Result: {0}\nHost: {1}".format(result, socket.gethostname())
        utility.send_message(subject, message)

    try:

        main(main_config, job_config, job_name)
        utility.update_job_fails(job_name=job_name, success=True)

        # subject = "SyncGisServices - SUCCESS"
        # result = "Successfully synced services."
        # message = "Result: {0}\nHost: {0}".format(result, socket.gethostname())
        # Utility.send_message(subject, message)

    except Exception as e:

        logger.exception("Fatal error in main process.")

        fail_count = utility.update_job_fails(job_name=job_name, success=False)
        skip_fails = job_config['notification']['skip_fails']

        if fail_count > skip_fails:

            subject = "SyncGisServices - ERROR"
            result = "An error occurred while syncing services."
            log_path = main_config['logging']['handlers']['fileHandler']['filename']
            message = "Result: {0}\nLog: {1}\nHost: {2}".format(result, log_path, socket.gethostname())
            utility.send_message(subject, message)

sys.exit()
