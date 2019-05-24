import os
import json
import urllib
import logging
import requests
from uuid import uuid4
from agstools.utility import merge_dicts, chunk_iterable, features_as_json

logger = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class FeatureLayer(object):
    """Perform simple operations using json on ArcGIS feature layer rest endpoint.

    See ArcGIS REST API documentation for query*, add*, delete*, and update* method parameters.
    """

    def __init__(self, url, token='', certificate=None, out_sr='', out_path=''):
        """
        Class initializer.

        :param url: <str> Feature service layer REST endpoint URL
        :param token: <str> ArcGIS Server or Portal authentication token
        :param certificate: <str> Path to certificate file (.pem)
        :param out_sr: <str> EPSG spatial reference WKID
        :param out_path: <str> Path to workspace for data storage
        """

        self.url = url
        self.token = token
        self.certificate = certificate
        self.params = {'f': 'json', 'token': self.token, 'outSR': out_sr}
        self.uid = str(uuid4())
        self.json_path = os.path.join(out_path, self.uid + '.json') if out_path != '' else ''

    def __make_request(self, url, method, params={}):
        """
        Return json result of request to service endpoint

        :param url: <str> URL for request
        :param method: <str> One of 'GET' or 'POST'
        :param params: <str> URL query string parameters
        :return: <requests.Response> Request response
        """

        response = None
        # merge passed params with class default params; passed params override
        request_params = merge_dicts(self.params, params)

        with requests.session() as s:

            if self.certificate is not None:
                s.verify = self.certificate

            if method.lower() == 'get':
                response = s.get(url=url, params=request_params)
            elif method.lower() == 'post':
                response = s.post(url=url, data=request_params)

        # check to see if an unsupported http method type was used
        if response is None:
            raise Exception('Request URL: {0} | Method type {1} not supported'.format(url, method))

        # check for an error in the service response
        if response.json().get('error'):
            raise Exception('Request URL: {0} | Service error: {1}'.format(url, response.json().get('error')))

        return response

    def attachments_info(self):
        """
        Get attachments info for feature layer.

        :return: <dict> Attachment info
        """

        attachments_info = {}

        oid_response = self.query(where='1=1', returnIdsOnly=True).json()
        objectids = oid_response['objectIds']

        for oid in objectids:
            attachments_url = urllib.parse.urljoin(self.url, '{0}/attachments'.format(oid))
            attachments_response = self.__make_request(attachments_url, 'get')
            attachments_data = attachments_response.json()
            attachments_infoitems = attachments_data['attachmentInfos']

            if len(attachments_infoitems) > 0:
                attachments_info[oid] = attachments_infoitems

        return attachments_info

    def definition(self):
        """
        Get json feature service definition.

        :return: <dict> JSON feature service layer definition
        """

        url = urllib.parse.urljoin(self.url, '')
        return self.__make_request(url, 'get', params={}).json()

    def query(self, **params):
        """
        Get query result from feature layer.

        https://developers.arcgis.com/rest/services-reference/query-feature-service-.htm

        :param params: <dict> Feature service query operation supported parameters
        :return: <requests.Response> Request response object
        """

        url = urllib.parse.urljoin(self.url, 'query')
        return self.__make_request(url, 'get', params)

    def query_features(self, **params):
        """
        Get JSON features from feature layer query.

        https://developers.arcgis.com/rest/services-reference/query-feature-service-.htm

        :param params: <dict> Feature service query operation supported parameters
        :return: <list> List of JSON features
        """

        return self.query(**params).json()['features']

    def query_features_batch(self, n=500, **params):
        """
        Get JSON features from feature layer query in batches of size n.

        This method should typically be used instead of .query_features()

        :param n: <int> Batch size
        :param params: <dict> Feature service query operation supported parameters
        :return: <list> List of JSON features
        """

        result = []

        oid_response = self.query(returnIdsOnly=True, **params).json()

        if oid_response['objectIds']:

            oid_field = oid_response['objectIdFieldName']
            oid_values = sorted(oid_response['objectIds'])
            oid_chunks = [c for c in chunk_iterable(oid_values, n)]

            for c in oid_chunks:

                where_clause = '{0} >= {1} AND {0} <= {2}'.format(oid_field, min(c), max(c))
                params['where'] = where_clause
                features = self.query_features(**params)
                result += features

        return result

    def add_features(self, **params):
        """
        Add JSON features to feature layer.

        Features should not include OID field

        https://developers.arcgis.com/rest/services-reference/add-features.htm

        :param params: <dict> Feature service add operation supported parameters
        :return: <requests.Response> Request response object
        """

        url = urllib.parse.urljoin(self.url, 'addFeatures')
        return self.__make_request(url, 'post', params)

    def add_features_batch(self, n=500, **params):
        """
        Add JSON features to feature layer in batches of size n.

        This method should typically be used instead of .add_features()

        :param n: <int> Batch size
        :param params: <dict> Feature service add operation supported parameters
        :return: <requests.Response> Request response object (for last request)
        """

        features = params.get('features')
        feature_chunks = [c for c in chunk_iterable(features, n)]

        for c in feature_chunks:

            params['features'] = features_as_json(c)
            result = self.add_features(**params)

        return result

    def update_features(self, **params):
        """
        Update JSON features in feature layer.

        Features should include OID field

        :param params: <dict> Feature service update operation supported parameters
        :return: <requests.Response> Request response object
        """

        url = urllib.parse.urljoin(self.url, 'updateFeatures')
        return self.__make_request(url, 'post', params)

    def update_features_batch(self, n=500, **params):
        """
        Update json features in feature layer in batches of size n.

        This method should typically be used instead of .update_features()

        :param n: <int> Batch size
        :param params: <dict> Feature service update operation supported parameters
        :return: <requests.Response> Request response object (for last request)
        """

        features = params.get('features')
        feature_chunks = [c for c in chunk_iterable(features, n)]

        for c in feature_chunks:

            params['features'] = features_as_json(c)
            result = self.update_features(**params)

        return result

    def delete_features(self, **params):
        """
        Delete features from feature layer.

        :param params: <dict> Feature service delete operation supported parameters
        :return: <requests.Response> Request response object
        """

        url = urllib.parse.urljoin(self.url, 'deleteFeatures')
        return self.__make_request(url, 'post', params)

    def export_features_json(self, features):
        """
        Write json features to disk.

        Writes to self.json_path as [self.uid].json

        :param features: <list> Features
        :return: None
        """

        with open(self.json_path, 'w') as f:
            f.write(json.dumps(features))

    def remove_features_json(self):
        """
        Delete json features from disk.

        Deletes [self.uid].json from self.json_path

        :return: None
        """

        os.remove(self.json_path)
