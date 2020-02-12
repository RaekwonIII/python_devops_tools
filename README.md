# utils
[![Build Status](https://travis-ci.com/ScarfaceIII/utils.svg?branch=master)](https://travis-ci.com/ScarfaceIII/utils)

Small library with a utility to auto-version code, a utility to build Docker images of monorepo projects and a few 
classes to interact with Azure services.

* To use this library, add this to your Pipfile:
```bash
[[source]]
url = "https://${FURY_AUTH}@pypi.fury.io/scarfaceiii/"
verify_ssl = true
name = "fury"
```
Or similar configuration for pip or other services.

* Then install with `pipenv install scarface-utils`
* Classes and methods can be accessed as such:
```python
from scarface_utils.azure_utils.azure_storage import AzureStorage
azure_storage = AzureStorage.from_key_vault_config('config.cfg')
```
* Finally, the auto-version tool is available as a command line script:
```bash
./bump-version
```
