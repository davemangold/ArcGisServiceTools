import os
import re
import urllib
import shutil
import logging
from copy import deepcopy
from datetime import datetime

logger = logging.getLogger(__name__)


class AttachmentRetriever(object):
    """Retrieve attachments from features in a feature layer and save to file system."""

    def __init__(self, feature_layer, out_path='', out_hierarchy=[]):

        self.feature_layer = feature_layer
        self.oid_field = self.feature_layer.definition()['objectIdField']
        self.out_path = out_path
        self.out_hierarchy = out_hierarchy

    def __sanitize_string(self, value):
        """
        Return value with invalid filepath characters removed or replaced.

        :param value: <str> Value to sanitize
        :return: <str> Value
        """

        value = re.sub('[^\w\s-]', '', value).strip()

        return value

    def __format_feature(self, feature):
        """
        Convert values (like dates) to a user-friendly format.

        :param feature: <dict> JSON feature as dict
        :return: <dict> Feature
        """

        fields = self.feature_layer.definition()['fields']
        date_field_names = [field['name'] for field in fields if field['type'] == 'esriFieldTypeDate']

        # convert dates to formatted strings
        for attr_name in feature['attributes'].keys():

            if attr_name in date_field_names:
                time_stamp = feature['attributes'][attr_name]

                if time_stamp:
                    time = datetime.fromtimestamp(time_stamp / 1e3)
                    time_str = time.strftime('%Y-%m-%d')
                    feature['attributes'][attr_name] = time_str

        # convert all attributes to strings and sanitize
        for k, v in feature['attributes'].items():
            if k != self.oid_field:
                feature['attributes'][k] = self.__sanitize_string(str(v))

        return feature

    def __get_attachment_folders(self, feature):
        """
        Return a list of the folders in the folder hierarchy for this features attachments.

        :param feature: <dict> JSON feature as dict
        :return: <tuple> Folder names
        """

        folder_names = tuple([feature['attributes'][attribute_name] for attribute_name in self.out_hierarchy])

        return folder_names

    def __build_attachment_path(self, attachment_folders):
        """
        Create the folder hierarchy for this features attachments if it doesn't already exist.

        :param attachment_folders: <iter> Iterable of folder names
        :return: None
        """

        os.chdir(self.out_path)

        for folder_name in attachment_folders:
            this_path = os.path.join(os.getcwd(), folder_name)
            try:
                os.mkdir(folder_name)
            except OSError as e:
                logger.debug("Folder already exists: {0}".format(this_path))
                raise e
            os.chdir(folder_name)

    def __download_attachment(self, url, filepath):
        """
        Save the resource at url to filepath.

        :param url: <str> Feature service URL
        :param filepath: <str> Target filepath
        :return: None
        """

        auth_url = url + '?token=' + self.feature_layer.token
        urllib.request.urlretrieve(auth_url, filepath)

    def __get_attachment_data(self):
        """
        Return a dict of feature info and associated attachments.

        Only includes data for features with attachments.

        :return: <dict> Attachment data
        """

        attachment_data = {}

        query_fields = ','.join([self.oid_field] + self.out_hierarchy)
        attachment_infos = self.feature_layer.attachments_info()
        features = self.feature_layer.query_features_batch(where='1=1', outFields=query_fields)
        formatted_features = [self.__format_feature(f) for f in deepcopy(features)]

        for feature in formatted_features:
            feature_oid = feature['attributes'][self.oid_field]

            try:
                feature_attachments = attachment_infos[feature_oid]
            except KeyError:
                logger.debug("No attachments for feature: {0}".format(feature_oid))
                continue

            if len(feature_attachments) > 0:

                for attachment_info in feature_attachments:
                    attachment_id = attachment_info['id']
                    attachment_name = attachment_info['name']
                    attachment_folders = self.__get_attachment_folders(feature)

                    attachment_item = {
                        'name': attachment_name,
                        'url': urllib.parse.urljoin(
                            self.feature_layer.url,
                            '/'.join([str(feature_oid), 'attachments', str(attachment_id)]))}

                    if attachment_folders in attachment_data:
                        attachment_data[attachment_folders].append(attachment_item)
                    else:
                        attachment_data[attachment_folders] = [attachment_item]

        return attachment_data

    def save_attachments(self):
        """
        Save all attachments from self.feature_layer features to disk.

        :return: None
        """

        download_count = 0
        attachment_data = self.__get_attachment_data()

        for attachment_folders in attachment_data:
            attachment_path = os.path.join(self.out_path, *attachment_folders)

            if not os.path.isdir(attachment_path):
                self.__build_attachment_path(attachment_folders)

            for attachment_item in attachment_data[attachment_folders]:
                attachment_name = attachment_item['name']
                attachment_url = attachment_item['url']
                attachment_filepath = os.path.join(attachment_path, attachment_name)

                if not os.path.exists(attachment_filepath):
                    self.__download_attachment(attachment_url, attachment_filepath)
                    download_count += 1

        logger.debug("Attachment files downloaded: {0}".format(download_count))
