import json
import urllib
import contextlib
from dateutil import tz


def parse_connection_string(connection_string):
    """
    Return dict from formatted connection string items.

    :param connection_string: <str> formatted connection string
    :return: <dict>
    """

    data = {}

    for param in connection_string.split(';'):
        name, value = param.split('=')
        data[name] = value

    return data


def submit_request(request):
    """
    Return the response from an HTTP request in json format.

    :param request: <urllib.request.Request>
    :return: <dict>
    """

    with contextlib.closing(urllib.request.urlopen(request)) as response:
        content = response.read()
        content_decoded = content.decode("utf-8")
        job_info = json.loads(content_decoded)
        return job_info


def get_token(token_url, username, password):
    """
     Return an authentication token from ArcGIS Online or ArcGIS Server.

    :param token_url: <str> token request REST endpoint
    :param username: <str> admin username
    :param password: <str> admin password
    :return: <str>
    """

    if token_url is not None:
        url_parts = urllib.parse.urlparse(token_url)
        host_url = url_parts.scheme + '://' + url_parts.netloc

        params = {"username": username,
                  "password": password,
                  "referer": host_url,
                  "f": "json"}

        data = urllib.parse.urlencode(params)
        data_encoded = data.encode("utf-8")
        request = urllib.request.Request(token_url, data=data_encoded)
        token_response = submit_request(request)

        if "token" in token_response:
            token = token_response.get("token")
            return token
        else:
            if "error" in token_response:
                error_mess = token_response.get("error", {}).get("message")
                if "This request needs to be made over https." in error_mess:
                    token_url = token_url.replace("http://", "https://")
                    token = get_token(token_url, username, password)
                    return token
                else:
                    raise Exception("Portal error: {} ".format(error_mess))


def merge_dicts(x, y):
    """
    Merge two dictionaries. Values from y will be retaining where keys match.

    :param x: <dict> first dictionary
    :param y: <dict> second dictionary
    :return: <dict>
    """

    z = x.copy()
    z.update(y)
    return z


def chunk_iterable(i, n):
    """
    Yield n-sized chunks from iterable i.

    :param i: <iter> any iterable type
    :param n: <int> chunk size
    :return: <iterator>
    """

    for j in range(0, len(i), n):
        yield i[j:j+n]


def features_as_json(features=[]):
    """
    Return list of features as json string.

    :param features: <list> features from arcgis service
    :return: <str>
    """

    return json.dumps(features)


def geom_esri_to_geojson(esri_geom_type):
    """
    Return GeoJSON equivalent of ESRI geometry type.

    :param esri_geom_type: <str> ESRI description of geometry type
    :return: <str>
    """

    geom_type_map = {'esriGeometryPoint': 'Point',
                     'esriGeometryMultipoint': 'MultiPoint',
                     'esriGeometryPolyline': 'LineString',
                     'esriGeometryPolygon': 'Polygon'}
    try:
        return geom_type_map[esri_geom_type]

    except IndexError:
        raise Exception("There is no conversion for the specified ESRI geometry type.")


def utc_to_pacific(datetime_utc):
    """
    Returns a PST datetime object from UTC datetime object.

    :param datetime_utc: <datetime.datetime> UTC datetime object
    :return: <datetime.datetime>
    """

    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/Los_Angeles')

    datetime_utc = datetime_utc.replace(tzinfo=from_zone)
    datetime_pst = datetime_utc.astimezone(to_zone)

    return datetime_pst


def pacific_to_utc(datetime_pst):
    """
    Returns a UTC datetime object from PST datetime object.

    :param datetime_pst: <datetime.datetime> PST datetime object
    :return: <datetime.datetime>
    """

    from_zone = tz.gettz('America/Los_Angeles')
    to_zone = tz.gettz('UTC')

    datetime_pst = datetime_pst.replace(tzinfo=from_zone)
    datetime_utc = datetime_pst.astimezone(to_zone)

    return datetime_utc