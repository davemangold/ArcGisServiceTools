import logging
from copy import deepcopy
from feature_processor import FeatureProcessor
from attribute_mapper import AttributeMapper
from utility import merge_dicts

logger = logging.getLogger('FeatureSyncer')


class FeatureSyncer(object):
    """Sync features between feature layers."""

    def __init__(self, src_feat_layer, tgt_feat_layer, custom_attr_mapper=None):

        self.src_feat_layer = src_feat_layer
        self.tgt_feat_layer = tgt_feat_layer
        self.cust_attr_mapper = custom_attr_mapper if isinstance(custom_attr_mapper, AttributeMapper) else AttributeMapper()
        self.auto_attr_mapper = self.__build_auto_attr_mapper()
        self.comp_features = {'src': {'index': {}, 'matched': [], 'unmatched': []},
                              'tgt': {'index': {}, 'matched': [], 'unmatched': []}}

    def __build_auto_attr_mapper(self):
        """Return attribute mapper for matching source and target field names."""

        auto_mapper = AttributeMapper()
        src_fields = [f['name'] for f in self.src_feat_layer.definition()['fields']]
        tgt_fields = [f['name'] for f in self.tgt_feat_layer.definition()['fields']]

        for f_name in list(set(src_fields) & set(tgt_fields)):
            auto_mapper.add_mapping(f_name, f_name)

        return auto_mapper

    def __reset_comp_features(self):

        self.comp_features = {'src': {'index': {}, 'matched': [], 'unmatched': []},
                              'tgt': {'index': {}, 'matched': [], 'unmatched': []}}

    def __comp_features(self, src_uid_field, tgt_uid_field):
        """Return dict of source and target matched and unmatched features."""

        # remove previously compared features
        self.__reset_comp_features()

        # get current attribute map
        attr_map = self.__get_attr_map()

        # get source feat layer attributes from attr map
        src_attr = [k for k, v in sorted(attr_map.items())]
        # get target feat layer attributes from attr map
        tgt_attr = [v for k, v in sorted(attr_map.items())]

        # get source and target json features
        src_features = self.src_feat_layer.query_features_batch(where='1=1', outFields=', '.join(src_attr))
        tgt_features = self.tgt_feat_layer.query_features_batch(where='1=1', outFields=', '.join(tgt_attr))

        # build feature indexes with uid field as key, oid field as value
        self.comp_features['src']['index'] = {f['attributes'][src_uid_field]: f['attributes']['OBJECTID']
                                              for f in src_features}
        self.comp_features['tgt']['index'] = {f['attributes'][tgt_uid_field]: f['attributes']['OBJECTID']
                                              for f in tgt_features}

        # process matched and exclusive features from source and target feature sets
        self.comp_features['src']['matched'] = [
            f for f in src_features if f['attributes'][src_uid_field] in self.comp_features['tgt']['index']]
        self.comp_features['src']['unmatched'] = [
            f for f in src_features if f['attributes'][src_uid_field] not in self.comp_features['tgt']['index']]
        self.comp_features['tgt']['matched'] = [
            f for f in tgt_features if f['attributes'][tgt_uid_field] in self.comp_features['src']['index']]
        self.comp_features['tgt']['unmatched'] = [
            f for f in tgt_features if f['attributes'][tgt_uid_field] not in self.comp_features['src']['index']]

    def __sync_one_way(self, src_uid_field, tgt_uid_field):
        """Sync features service features based on uid field matching.

        Feature in source not in target: feature added to target from source
        Feature in target not in source: delete feature from target
        Feature in source and target: update feature in target to match source

        src_uid_field -- the source feature layer unique id field name
        tgt_uid_field -- the target feature layer unique id field name
        """

        self.__comp_features(src_uid_field, tgt_uid_field)

        # get current attribute map
        attr_map = self.__get_attr_map()

        # make copies of update, add, and delete features before modification
        update_features = deepcopy(self.comp_features['src']['matched'])
        add_features = deepcopy(self.comp_features['src']['unmatched'])
        delete_features = deepcopy(self.comp_features['tgt']['unmatched'])

        logger.debug("Updating features ({0}).".format(len(update_features)))
        if len(update_features) > 0:
            # for update features, replace source OID with target OID (required by REST updateFeatures operation)
            for f in update_features:
                f['attributes']['OBJECTID'] = self.comp_features['tgt']['index'][f['attributes'][src_uid_field]]
            # create a feature processor to modify update features
            update_fp = FeatureProcessor(update_features)
            # remap field names
            update_fp.replace_attributes(attr_map)
            # update features in target feature layer
            self.tgt_feat_layer.update_features_batch(features=update_fp.features)

        logger.debug("Adding features ({0}).".format(len(add_features)))
        if len(add_features) > 0:
            # create a feature processor to modify add features
            add_fp = FeatureProcessor(add_features)
            # remap field names
            add_fp.replace_attributes(attr_map)
            # remove OID field (auto-generated on insert via REST addFeatures operation)
            add_fp.remove_attributes(['OBJECTID'])
            # add features to target feature layer
            self.tgt_feat_layer.add_features_batch(features=add_fp.features)

        logger.debug("Deleting features ({0}).".format(len(delete_features)))
        if len(delete_features) > 0:
            # create list of OIDs for target features to delete
            delete_oids = ', '.join([str(f['attributes']['OBJECTID']) for f in delete_features])
            # delete features from target feature layer
            self.tgt_feat_layer.delete_features(objectIds=delete_oids)

    def __sync_two_way(self, src_uid_field, tgt_uid_field, reconcile_type):
        """Sync features service features based on uid field matching.

        Feature in source not in target: feature added to target from source
        Feature in target not in source: feature added to source from target
        Feature in source and target: update feature in source or target based on reconcile type.

        src_uid_field -- the source feature layer unique id field name
        tgt_uid_field -- the target feature layer unique id field name
        reconcile_type -- the feature layer type - 'source' or 'target' - which will be favored for reconciliation
        """

        raise Exception('Two-way sync is not yet supported.')

    def __get_attr_map(self):
        """Return current, combined attribute map

        Custom attribute mapper fields override auto attribute mapper fields.
        """

        return merge_dicts(self.auto_attr_mapper.attribute_map, self.cust_attr_mapper.attribute_map)

    def sync(self, src_uid_field, tgt_uid_field, sync_type='one-way', reconcile_type='source'):
        """Sync features between two feature services."""

        if sync_type.lower() == 'one-way':
            self.__sync_one_way(src_uid_field, tgt_uid_field)
        elif sync_type.lower() == 'two-way':
            self.__sync_two_way(src_uid_field, tgt_uid_field, reconcile_type)
        else:
            raise Exception('Sync type {0} not recognized.'.format(sync_type))
