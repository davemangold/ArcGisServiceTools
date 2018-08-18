import json
import urllib
import smtplib
import contextlib
from datetime import datetime
from dateutil import tz
from email.mime.text import MIMEText


with open('config.json', 'r') as f:
    config = json.load(f)


def parse_connection_string(connection_string):

    data = {}

    for param in connection_string.split(';'):
        name, value = param.split('=')
        data[name] = value

    return data


def submit_request(request):
    """ Return the response from an HTTP request in json format."""

    with contextlib.closing(urllib.request.urlopen(request)) as response:
        content = response.read()
        content_decoded = content.decode("utf-8")
        job_info = json.loads(content_decoded)
        return job_info


def get_token(token_url, username, password):
    """ Return an authentication token from ArcGIS Online or ArcGIS Server."""

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


def get_service_token(svc_config):

    token = ''

    svc_type = svc_config['type']
    token_user = config['authentication'][svc_type]['user']
    token_password = config['authentication'][svc_type]['password']
    token_url = config['authentication'][svc_type]['token_url']

    if token_url is not None:
        token = get_token(token_url, token_user, token_password)

    return token


def get_service_certificate(svc_config):

    cert = None

    svc_type = svc_config['type']
    svc_cert = config['authentication'][svc_type]['certificate']

    if svc_cert is not None:
        cert = svc_cert

    return cert


def send_message(subject, message_text):

    mail_server = config['notification']['mail_server']
    sender = config['notification']['sender']
    user = config['notification']['user']
    password = config['notification']['password']
    recipients_string = config['notification']['recipients']
    recipients = recipients_string.split(",")

    msg = MIMEText(message_text)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["Reply-to"] = sender
    msg["To"] = recipients_string

    s = smtplib.SMTP()
    s.connect(mail_server)  # COMMENT OUT FOR TESTING
    # s.connect("127.0.0.1", 25)  # COMMENT OUT FOR PRODUCTION; PAPERCUT MAIL SERVER
    s.login(user, password)  # COMMENT OUT FOR TESTING
    s.sendmail(sender, recipients, msg.as_string())
    s.close()


def merge_dicts(x, y):
    """Merge two dictionaries."""

    z = x.copy()
    z.update(y)
    return z


def chunk_iterable(i, n):
    """Yield n-sized chunks from iterable i."""

    for j in range(0, len(i), n):
        yield i[j:j+n]


def features_as_json(features=[]):
    """Return list of features as json string."""

    return json.dumps(features)


def geom_esri_to_geojson(esri_geom_type):
    """Return GeoJSON equivalent of ESRI geometry type."""

    geom_type_map = {'esriGeometryPoint': 'Point',
                     'esriGeometryMultipoint': 'MultiPoint',
                     'esriGeometryPolyline': 'LineString',
                     'esriGeometryPolygon': 'Polygon'}
    try:
        return geom_type_map[esri_geom_type]

    except IndexError:
        raise Exception("There is no conversion for the specified ESRI geometry type.")


def utc_to_pacific(datetime_utc):
    """Accepts a UTC datetime object and returns a PST datetime object."""

    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/Los_Angeles')

    datetime_utc = datetime_utc.replace(tzinfo=from_zone)
    datetime_pst = datetime_utc.astimezone(to_zone)

    return datetime_pst


def pacific_to_utc(datetime_pst):
    """Accepts a PST datetime object and returns a UTC datetime object."""

    from_zone = tz.gettz('America/Los_Angeles')
    to_zone = tz.gettz('UTC')

    datetime_pst = datetime_pst.replace(tzinfo=from_zone)
    datetime_utc = datetime_pst.astimezone(to_zone)

    return datetime_utc


def update_job_fails(job_name, success):
    """Update the count of consecutive failures for the job.

    job_name <str>: name of the job
    success <bool>: True if job succeeded, otherwise False
    """

    job_exists = False
    lines = []

    with open('job_failures.txt', 'r') as input_file:
        for line in input_file:
            if job_name in line:
                job_exists = True
                if success is True:
                    fail_count = 0
                else:
                    fail_count = int(line.strip().split(':')[-1]) + 1
                new_line = '{0}: {1}\n'.format(job_name, fail_count)
                lines.append(new_line)
            else:
                lines.append(line)

        if job_exists is False:
            if success is True:
                fail_count = 0
            else:
                fail_count = 1
            add_line = '{0}: {1}\n'.format(job_name, fail_count)
            lines.append(add_line)

    with open('job_failures.txt', 'w') as output_file:
        output_file.writelines(lines)

    return fail_count
