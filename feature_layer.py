import os
import json
import urllib
import logging
import requests
from uuid import uuid4
from utility import merge_dicts, chunk_iterable, features_as_json

logger = logging.getLogger('FeatureLayer')


class FeatureLayer(object):
    """Perform simple operations using json on ArcGIS feature layer rest endpoint.

    See ArcGIS REST API documentation for query*, add*, delete*, and update* method parameters.
    """

    def __init__(self, config, svc_config, url, token='', certificate=None):
        self.config = config
        self.svc_config = svc_config
        self.url = url
        self.token = token
        self.certificate = certificate
        self.params = {'f': 'json', 'token': self.token, 'outSR': self.svc_config['out_sr']}
        self.uid = str(uuid4())
        self.json_path = os.path.join(self.config['workspaces']['json_storage'], self.uid + '.json')

    def __make_request(self, url, method, params):
        """Return json result of request to service endpoint

        Only GET and POST request methods are currently supported.
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
            raise Exception('Method type {0} not supported'.format(method))

        # check for an error in the service response
        if response.json().get('error'):
            raise Exception('Service error: {0}'.format(response.json().get('error')))

        return response

    def definition(self):
        """Get json feature service definition."""

        url = urllib.parse.urljoin(self.url, '')
        return self.__make_request(url, 'get', params={}).json()

    def query(self, **params):
        """Get query result from feature layer."""

        url = urllib.parse.urljoin(self.url, 'query')
        return self.__make_request(url, 'get', params)

    def query_features(self, **params):
        """Get json features from feature layer."""

        return self.query(**params).json()['features']

    def query_features_batch(self, n=500, **params):
        """Get json features from feature layer in batches of size n."""

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
        """Add json features to feature layer.

        Features should not include OID field
        """

        url = urllib.parse.urljoin(self.url, 'addFeatures')
        return self.__make_request(url, 'post', params)

    def add_features_batch(self, n=500, **params):
        """Add json features to feature layer in batches of size n."""

        features = params.get('features')
        feature_chunks = [c for c in chunk_iterable(features, n)]

        for c in feature_chunks:

            params['features'] = features_as_json(c)
            result = self.add_features(**params)

        return result

    def update_features(self, **params):
        """Update json features in feature layer.

        Features should include OID field
        """

        url = urllib.parse.urljoin(self.url, 'updateFeatures')
        return self.__make_request(url, 'post', params)

    def update_features_batch(self, n=500, **params):
        """Add json features to feature layer in batches of size n."""

        features = params.get('features')
        feature_chunks = [c for c in chunk_iterable(features, n)]

        for c in feature_chunks:

            params['features'] = features_as_json(c)
            result = self.update_features(**params)

        return result

    def delete_features(self, **params):
        """Delete features from feature layer."""

        url = urllib.parse.urljoin(self.url, 'deleteFeatures')
        return self.__make_request(url, 'post', params)

    def export_features_json(self, features):
        """Write json features to disk."""

        with open(self.json_path, 'w') as f:
            f.write(json.dumps(features))

    def remove_features_json(self):
        """Delete json features from disk."""

        os.remove(self.json_path)
