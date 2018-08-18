import json
import logging
import requests
import urllib
import pypyodbc
import utility

logger = logging.getLogger('ServiceCataloger')

with open('config.json', 'r') as f:
    main_config = json.load(f)

with open('update_service_catalog.json', 'r') as f:
    script_config = json.load(f)


class ServiceRecord(object):

    def __init__(self, database, dataset, service_name, service_type, service_url):
        self.database = database
        self.dataset = dataset
        self.service_name = service_name
        self.service_type = service_type
        self.service_url = service_url

    def as_tuple(self):

        return (self.database,
                self.dataset,
                self.service_name,
                self.service_type,
                self.service_url)


class ServiceCataloger(object):
    """Updates SQL Server table of services and associated feature class locks."""

    def __init__(self, admin_services_root):
        self.admin_services_root = admin_services_root
        self.manifest_path = 'iteminfo/manifest/manifest.json'
        self.service_types = ['FeatureServer', 'MapServer']
        self.skip_folders = ['System']
        self.skip_names = ['raster_data']
        self.server_config = main_config['authentication']['cws_ags']
        self.certificate = self.server_config['certificate']
        self.token_url = self.server_config['token_url']
        self.token_user = self.server_config['user']
        self.token_password = self.server_config['password']
        self.token = utility.get_token(self.token_url, self.token_user, self.token_password)
        self.session = requests.session()
        self.session.verify = self.certificate if self.certificate is not None else False

    def __del__(self):

        self.session.close()

    def __get_database_connection(self, database_name):
        """Return pypyodbc connection to named database (configured in config.py)."""

        db_config = script_config['database'][database_name.lower()]
        connection = pypyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER='      + db_config['server']   + ';'
            r'DATABASE='    + db_config['database'] + ';'
            r'UID='         + db_config['username'] + ';'
            r'PWD='         + db_config['password'])

        return connection

    def __make_request(self, request_url):

        return self.session.get(url=request_url, params={'f': 'json', 'token': self.token}).json()

    def __build_service_records(self, catalog_url):

        service_records = []
        catalog = self.__make_request(catalog_url)
        services = catalog['services']

        for service in services:

            svc_type = service['type']

            if svc_type in self.service_types:
                svc_name = service['serviceName']
                svc_url = urllib.parse.urljoin(catalog_url + '/', service['serviceName'] + '.' + service['type'])
                svc_manifest_url = urllib.parse.urljoin(svc_url + '/', self.manifest_path)
                svc_manifest = self.__make_request(svc_manifest_url)
                if svc_manifest.get('status') == 'error':
                    logger.warning("Manifest unavailable for service: {0}".format(svc_name))
                    continue
                svc_databases = svc_manifest['databases']

                for database in svc_databases:
                    conn_string = database['onPremiseConnectionString']
                    conn_data = utility.parse_connection_string(conn_string)
                    database_name = conn_data['DATABASE']

                    if database['onServerName'] not in self.skip_names:
                        for dataset in database['datasets']:
                            dataset_name = dataset['onServerName']
                            svc_record = ServiceRecord(database_name, dataset_name, svc_name, svc_type, svc_url)
                            service_records.append(svc_record)

        return service_records

    def __get_service_records(self):

        service_records = []

        root_catalog = self.__make_request(self.admin_services_root)
        root_folders = root_catalog['folders']

        for folder_name in root_folders:

            if folder_name not in self.skip_folders:

                catalog_url = urllib.parse.urljoin(self.admin_services_root, folder_name)
                service_records += self.__build_service_records(catalog_url)

        return service_records

    def update_catalog(self):
        """Update the service catalog table in the database."""

        service_records = [rec.as_tuple() for rec in self.__get_service_records()]
        database_connection = self.__get_database_connection('utility')
        cursor = database_connection.cursor()

        # empty the existing table
        query_truncate = "TRUNCATE TABLE Utility.dbo.ServiceCatalog"
        cursor.execute(query_truncate)

        # populate the table with current data
        query_insert = ("INSERT INTO Utility.dbo.ServiceCatalog ("
                        "DatabaseName, DatasetName, ServiceName, ServiceType, ServiceUrl) "
                        "VALUES (?, ?, ?, ?, ?)")
        cursor.executemany(query_insert, service_records)
        cursor.commit()
