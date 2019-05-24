import json
import smtplib
import logging
from copy import deepcopy
from datetime import datetime
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


class FeatureMailer(object):

    def __init__(self, mail_server, username, password, feature_syncer, mailer_config):
        """
        Class initializer.

        :param mail_server: <str> mail server name
        :param username: <str> mail server username
        :param password: <str> mail server password
        :param feature_syncer: <feature_syncer.FeatureSyncer> FeatureSyncer for which mail will be sent
        :param mailer_config: <dict> mailer config from job config
        """

        self.mail_server = mail_server
        self.username = username
        self.password = password
        self.mailer_config = mailer_config
        self.feature_syncer = feature_syncer
        self.conn = smtplib.SMTP()

    def __recipient_from_attr(self, feature):
        """
        Return value from configured recipient attribute (msg_to_attr).

        :param feature: <dict> JSON feature as dict
        :return: <str> Applicant email
        """

        return feature['attributes'].get(self.mailer_config.get('msg_to_attr'))

    def __build_message(self, feature):
        """
        Construct message text by inserting dynamically generated body between configured header and footer.

        :param feature: <dict> JSON feature as dict
        :return: <str> Message
        """

        drop_attr = self.mailer_config['drop_attr']
        header = self.mailer_config['msg_header']
        footer = self.mailer_config['msg_footer']
        body = '\n'.join(['{0}: {1}'.format(k, v) for k, v in sorted(feature['attributes'].items())
                          if k not in drop_attr])
        return '\n\n'.join([header, body, footer])

    def __send_message(self, message, feature):
        """
        Send a message to the configured recipients.Send a message to the configured recipients.

        :param message: <str> Message to send
        :param feature: <dict> JSON feature as dict
        :return: None
        """

        sender = self.mailer_config['msg_from']
        recipients = self.mailer_config.get('msg_to')
        add_recipient = self.__recipient_from_attr(feature)
        if add_recipient:
            recipients = ','.join([recipients, add_recipient])
        recipients_list = recipients.split(',')

        msg = MIMEText(message)
        msg["Subject"] = self.mailer_config['msg_subject']
        msg["From"] = sender
        msg["Reply-to"] = sender
        msg["To"] = recipients

        self.conn.sendmail(sender, recipients_list, msg.as_string())

    def __format_feature(self, feature, feature_type):
        """
        Format feature for insertion into email message.

        :param feature: <dict> JSON feature as dict
        :param feature_type: One of 'src' or 'tgt'
        :return: <dict> Feature
        """

        fields = None
        working = deepcopy(feature)

        if feature_type.lower() == 'src':
            fields = self.feature_syncer.src_feat_layer.definition()['fields']
        elif feature_type.lower() == 'tgt':
            fields = self.feature_syncer.tgt_feat_layer.definition()['fields']
        else:
            raise Exception('type {0} is not recognized'.format(feature_type))

        result = self.__format_dates(working, fields)
        # add additional format methods here
        # format methods can be chained using dot notation within parentheses

        return result

    def __format_dates(self, feature, fields):
        """
        Convert unix timestamps to strings.

        :param feature: <dict> JSON feature as dict
        :param fields: <list> List of field names
        :return: <dict> Feature
        """

        date_field_names = [fld['name'] for fld in fields if fld['type'] == 'esriFieldTypeDate']
        for attr_name in feature['attributes'].keys():
            if attr_name in date_field_names:
                # get timestamp value
                time_stamp = feature['attributes'][attr_name]
                # only modify if value isn't null / None
                if time_stamp:
                    time = datetime.fromtimestamp(time_stamp / 1e3)
                    time_str = time.strftime('%m/%d/%Y %H:%M:%S')
                    feature['attributes'][attr_name] = time_str

        return feature

    def mail_features(self):
        """
        Mail feature reports to recipients based on configuration.

        :return: None
        """

        self.conn.connect(self.mail_server)
        self.conn.login(self.username, self.password)

        if self.mailer_config['send_mail']:
            if self.mailer_config['send_updated']:
                for f in self.feature_syncer.comp_features['src']['matched']:
                    f_form = self.__format_feature(f, 'src')
                    self.__send_message(self.__build_message(f_form), f)
            if self.mailer_config['send_added']:
                for f in self.feature_syncer.comp_features['src']['unmatched']:
                    f_form = self.__format_feature(f, 'src')
                    self.__send_message(self.__build_message(f_form), f)
            if self.mailer_config['send_deleted']:
                for f in self.feature_syncer.comp_features['tgt']['unmatched']:
                    f_form = self.__format_feature(f, 'tgt')
                    self.__send_message(self.__build_message(f_form), f)

        self.conn.close()
