import agstools
from agstools import FeatureLayer
from config import test_feature_layer as config
from unittest import TestCase


class TestFeatureLayer(TestCase):

    def setUp(self):
        """Setup the test suite."""

        token = agstools.utility.get_token(
            token_url=config.get('token_url'),
            username=config.get('username'),
            password=config.get('password'))

        self.feature_layer = FeatureLayer(
            url=config.get('layer_url'),
            token=token)

    def test_query_features_batch(self):
        """Test FeatureLayer query operations.

        FeatureLayer.query_features_batch() also calls .query_features() and .query()"""

        features = self.feature_layer.query_features_batch(where='1=1', fields='*')
        self.assertIsInstance(features, list)

    def test_definition(self):
        """Test FeatureLayer.definition() method."""

        definition = self.feature_layer.definition()
        self.assertTrue('geometryType' in definition.keys())
