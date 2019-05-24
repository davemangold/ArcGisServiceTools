import os
import json
import logging
from agstools.utility import geom_esri_to_geojson

logger = logging.getLogger(__name__)


class FeatureRetriever(object):
    """Retrieves features from feature layer and saves on disk."""

    def __init__(self, src_feat_layer, tgt_workspace, tgt_name, tgt_format):
        """
        Class initializer.

        :param src_feat_layer: <feature_layer.FeatureLayer> Source feature layer object
        :param tgt_workspace: <str> Path to output workspace
        :param tgt_name: <str> Name of output file
        :param tgt_format: <str> Output file format; one of 'esrijson' or 'geojson'
        """

        self.src_feat_layer = src_feat_layer
        self.tgt_workspace = tgt_workspace
        self.tgt_name = tgt_name
        self.tgt_format = tgt_format

    def __feature_json_to_geojson(self, feature, geometry_type):
        """
        Return input ArcGIS JSON feature reconfigured as GeoJSON.
        
        :param feature: <dict> JSON feature as dict
        :param geometry_type: GeoJSON geometry type; one of 'Point', 'Multipoint', 'LineString', 'Polygon'
        :return: <dict> GeoJSON feature as dict
        """

        feature['type'] = 'Feature'
        feature['properties'] = feature.pop('attributes')
        feature['geometry']['type'] = geometry_type  # Point, LineString, Polygon
        feature['geometry']['coordinates'] = feature['geometry'].pop('rings')
        return feature

    def __get_esri_json_container(self, out_fields="*"):
        """
        Return ESRI json object from feature layer with empty result set.

        :param out_fields: ESRI-formatted, comma-spearated string of field names
        :return: <dict> ESRI JSON container
        """

        return self.src_feat_layer.query(where='1=0', outFields=out_fields).json()

    def __get_geojson_container(self):
        """
        Return GeoJSON object template.

        :return: <dict> GeoJSON container
        """

        return {"type": "FeatureCollection",
                "features": []}

    def retrieve(self, where="1=1", out_fields="*", geometry=None, geometry_type=None):
        """
        Get source layer features and write to ESRI JSON or GeoJSON file.

        :param where: <str> ESRI where clause
        :param out_fields: <str> Comma-separated string of field names to include in output
        :param geometry: <dict> ESRI geometry, optional
        :param geometry_type: <str> ESRI geometry type, must be specified if using geometry
        :return: None
        """

        request_args = {'where': where,
                        'outFields': out_fields}
        if geometry is not None:
            request_args['geometry'] = str(geometry)
            request_args['geometryType'] = str(geometry_type)

        json_features = self.src_feat_layer.query_features_batch(**request_args)

        if self.tgt_format == 'esrijson':
            container = self.__get_esri_json_container(out_fields)
            outfile = os.path.join(self.tgt_workspace, self.tgt_name + '.json')

            for f in json_features:
                container['features'].append(f)

        elif self.tgt_format == 'geojson':
            esri_geom_type = self.src_feat_layer.definition()['geometryType']
            geojson_geom_type = geom_esri_to_geojson(esri_geom_type)
            container = self.__get_geojson_container()
            outfile = os.path.join(self.tgt_workspace, self.tgt_name + '.geojson')

            for f in json_features:
                container['features'].append(self.__feature_json_to_geojson(f, geojson_geom_type))

        else:
            raise Exception('Output format {0} not recognized.'.format(self.tgt_format))

        with open(outfile, 'w') as f:
            f.write(json.dumps(container))
