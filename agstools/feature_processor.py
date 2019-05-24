import logging

logger = logging.getLogger(__name__)


class FeatureProcessor(object):
    """Manipulate esri json features in a feature set."""

    def __init__(self, features):
        """
        Class initializer.

        :param features: <list> JSON features as list of dicts
        """

        self.features = features

    def remove_attributes(self, attribute_names=[]):
        """
        Remove attributes from features in self.features.

        attribute_names = {name_1, ..., name_n}

        :param attribute_names: <list> Attribute names
        :return: None
        """

        for f in self.features:
            for a in attribute_names:
                f['attributes'].pop(a)

    def add_attributes(self, attribute_map={}):
        """
        Add attributes (with default values) to features in self.features.

        attribute_map = {name_1: default_value_1, ..., name_n: default_value_n}

        :param attribute_map: <dict> Map of attribute names to default values
        :return: None
        """

        for f in self.features:
            for k, v in attribute_map.items():
                if k not in f['attributes']:
                    f['attributes'][k] = v
                else:
                    raise Exception('Field {0} already exists in feature'.format(k))

    def replace_attributes(self, replace_map={}):
        """
        Replace attributes names with new attribute names, keeping values.

        replace_map = {old_name_1: new_name_1, ..., old_name_n: new_name_n}

        :param replace_map: <dict> Map of old attribute names to new attribute names
        :return: None
        """

        clean_map = {k: v for k, v in replace_map.items() if k != v}
        new_attribute_map = {v: None for v in clean_map.values()}
        old_attribute_names = clean_map.keys()
        self.add_attributes(new_attribute_map)
        for f in self.features:
            for k, v in clean_map.items():
                f['attributes'][v] = f['attributes'][k]
        self.remove_attributes(old_attribute_names)

    def replace_values(self, value_map={}):
        """
        Replace all occurrences of old values with new values.

        value_map = {old_value_1: new_value_1, ..., old_value_n, new_value_n}

        :param value_map: <dict> Map of old values to new values
        :return: None
        """

        old_values = value_map.keys()

        for f in self.features:
            for k, v in f['attributes'].items():
                if v in old_values:
                    f['attributes'][k] = value_map[v]

    def set_values(self, attribute_name, value):
        """
        Set the value of an attribute for all features.

        :param attribute_name: <str> Attribute name
        :param value: <object> Attribute value
        :return: None
        """

        for f in self.features:
            f['attributes'][attribute_name] = value
