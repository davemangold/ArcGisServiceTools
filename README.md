# ArcGIS Service Tools

ArcGIS Service Tools is a collection of classes and utilities for working with ArcGIS services. For help with packaging, see the official Python [packaging guide](https://packaging.python.org/tutorials/packaging-projects/). The package name is prepended with cws- to indicate that this is a local package (not on PyPI). When installing with pip, use the package name "cws-agstools". When importing in a Python module, use the package name "agstools". See install command and example usage below. After building the package, files (including packaged dependencies) must be manually moved from the dist folder to the local package index.

__Test using (Python 3):__
```
$> cd C:\git\cws-agstools
$> python.exe -m unittest discover -s test
```

__Build using (Python 3):__
```
$> cd C:\git\cws-agstools
$> python.exe setup.py sdist bdist_wheel
```

__Install using (Python 3):__
```
$> python.exe -m pip install cws-agstools --no-index --find-links "C:\git\cws-agstools"
```

__Uninstall using (Python 3):__
```
$> python.exe -m pip uninstall cws-agstools
```

__Example usage:__
```python
import agstools
from agstools import FeatureLayer

token_url = "https://url/to/portal/generateToken"   # replace with valid url
username = "ihavepermission"                        # replace with valid username
password = "iamsecure"                              # replace with valid password

token = agstools.utility.get_token(
    token_url=token_url,
    username=username,
    password=password)

layer_url = "https://url/to/feature/service/layer"

feature_layer = FeatureLayer(
    url=layer_url,
    token=token)

features = feature_layer.query_features_batch(where='1=1', fields='*')
```
