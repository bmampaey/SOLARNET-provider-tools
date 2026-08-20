# Creating SVO resources with provider_tools

Providers are responsible for managing four types of resources in the SVO:

* the dataset description
* the keyword descriptions
* the metadata resources
* the associated data_location resources

For the dataset and keyword descriptions, we recommend using the [SVO admin web interface](https://solarnet.oma.be/service/admin/), as it is easier to navigate.

For metadata and data_location resources, however, there can be thousands of resources to create. Using a web interface for this can quickly become cumbersome. For this reason, we recommend creating a Python script to generate and submit these resources.

The `provider_tools` package was developed to make this easier.

This page takes you through a complete example, step by step. You do not need to be an advanced Python programmer to follow it.

## Our example

We will use the following situation throughout this example:

* Our dataset is called `MyDataset`.
* The FITS files are stored on the computer or server where the script will run.
* The FITS files are stored under `/data/`.
* Files are organised in subdirectories by year and month, for example, the FITS file for an observation on 21 July 2025 at 12:00:00 is `/data/2025/07/20250721_120000.fits`

The exact file names and directory structure do **not** have to be like this. They are only examples.

At the end of the example, we will have a script that can:

1. read FITS files from the local computer;
2. extract the information needed for the SVO metadata resources;
3. create the corresponding data_location resources;
4. send those resources to the SVO server.

## 1. Create a Provider

The main object we will use is a `Provider`.

A provider takes care of the overall process:

* reading information from the data files;
* building the metadata and data_location resources;
* submitting those resources to the SVO server.

Because our FITS files are stored on the local computer, we will use `ProviderFromLocalFitsFile`.

This provider already knows how to extract metadata from a FITS header. However, it does not know how the corresponding `data_location` should be created for our particular dataset.

So our first task is to tell it how to create a `data_location`.

## 2. Define the data_location resource

A data_location resource describes where the actual data file can be found.

For our example, we need to provide the following information:

| Field         | Value in our example                                          |
| ------------- | ------------------------------------------------------------- |
| file_url      | `https://myserver.edu/mydataset/2025/07/20250721_120000.fits` |
| file_path     | `2025/07/20250721_120000.fits`                                |
| file_size     | Obtained automatically from the local file                    |
| thumbnail_url | `https://myserver.edu/mydataset/2025/07/20250721_120000.jpg`  |
| offline       | `False` (the default)                                         |

Some of these values are easy to determine automatically. Others depend on how your files are organised.

To tell the provider how to determine them, we create our own `DataLocation` class based on `DataLocationFromLocalFile`.

We can then define a `get` method for any value that needs special treatment, while leaving the other values to the default implementation.

```python
from provider_tools import DataLocationFromLocalFile, ProviderFromLocalFitsFile


class DataLocation(DataLocationFromLocalFile):
    # Base directory used to determine the relative file_path.
    # The trailing / is required.
    BASE_FILE_PATH = '/data/'

    # Base URL used to build the file_url.
    # The trailing / is required.
    BASE_FILE_URL = 'https://myserver.edu/mydataset/'

    # The default implementation cannot determine the thumbnail URL,
    # so we define it here.
    def get_thumbnail_url(self):
        # Build the thumbnail URL from the FITS file URL.
        return self.get_file_url()[:-len('.fits')] + '.jpg'


class MyProvider(ProviderFromLocalFitsFile):
    DATA_LOCATION_CLASS = DataLocation
```

There are a few useful things to notice here.

### `file_path`

We want:

```text
2025/07/20250721_120000.fits
```

rather than:

```text
/data/2025/07/20250721_120000.fits
```

By setting:

```python
BASE_FILE_PATH = '/data/'
```

the default `get_file_path()` method knows which part of the local path should be removed.

### `file_url`

We want the public URL to be:

```text
https://myserver.edu/mydataset/2025/07/20250721_120000.fits
```

The default `get_file_url()` method builds this by combining `BASE_FILE_URL` with `file_path`.

Therefore:

```python
BASE_FILE_URL = 'https://myserver.edu/mydataset/'
```

is enough.

### `file_size`

We do not have to do anything for `file_size`.

Because we are using `DataLocationFromLocalFile`, the size from the local FITS file is determined automatically.

### `thumbnail_url`

The default implementation cannot know where your thumbnails are located, so we provide our own `get_thumbnail_url()` method.

In our example, the thumbnail has the same name as the FITS file, but with a `.jpg` extension.

### `offline`

We do not have to do anything here either. The default value is `False`.

## 3. Define the metadata resource

The previous example assumes that the FITS headers contain the information needed by the SVO.

If you follow the [SOLARNET metadata recommandations](https://solarnet-metadata.readthedocs.io/en/latest/), the code above may already be enough to get your provider working.

For this example, however, let's assume that our FITS files do **not** contain the recommended `DATE-BEG`, `DATE-END`, `WAVEMIN`, and `WAVEMAX` keywords.

Instead, our FITS headers contain:

* `DATE-OBS`: the start of the observation;
* `EXPTIME`: the exposure time, in seconds;
* `WAVELNTH`: the wavelength of the observation, in nm.

In the same way that we defined a `DataLocation` class, we will define a `Metadata` class with `get` methods for the [5 mandatory SVO fields](https://solarnet.oma.be/svo_data_provider_manual.html#creating-a-new-metadata-resource):

We therefore need to tell the provider how to obtain the [five mandatory SVO metadata fields](https://solarnet.oma.be/svo_data_provider_manual.html#creating-a-new-metadata-resource):

* `oid`
* `date_beg`
* `date_end`
* `wavemin`
* `wavemax`

We do this in much the same way as we did for `DataLocation`.

We create our own `Metadata` class based on `MetadataFromFitsHeader`, and then tell our provider to use it.

```python
from datetime import timedelta

from provider_tools import (
    DataLocationFromLocalFile,
    MetadataFromFitsHeader,
    ProviderFromLocalFitsFile,
)


class DataLocation(DataLocationFromLocalFile):
    # Same as before ...


class Metadata(MetadataFromFitsHeader):

    def get_date_beg(self):
        # DATE-OBS contains the start of the observation.
        return self.extract_field_value('date_obs')

    def get_date_end(self):
        # The end of the observation is the start time
        # plus the exposure time.
        return self.get_date_beg() + timedelta(
            seconds=self.extract_field_value('exptime')
        )

    def get_wavemin(self):
        # WAVELNTH is already expressed in nm.
        return self.extract_field_value('wavelnth')

    def get_wavemax(self):
        # For this example, the observation has a single wavelength.
        return self.extract_field_value('wavelnth')

    def get_oid(self):
        # Each observation must have a unique ID.
        # Here we use the observation date and time.
        return self.get_date_beg().strftime('%Y%m%d%H%M%S')


class MyProvider(ProviderFromLocalFitsFile):
    DATA_LOCATION_CLASS = DataLocation
    METADATA_CLASS = Metadata
```

### What does `extract_field_value()` do?

You do not need to read the FITS header yourself.

In the SVO, each metadata field is defined by a keyword description resource. This defines, among other things, the field name and its corresponding FITS keyword.

For example, the keyword description for `date_obs` defines `DATE-OBS` as its FITS keyword. Therefore:

```python
self.extract_field_value('date_obs')
```

asks `MetadataFromFitsHeader` to obtain the value corresponding to the `DATE-OBS` FITS keyword and convert it to the appropriate Python value. For a date/time field, this gives us a `datetime` value, which is why we can then write:

```python
self.get_date_beg() + timedelta(...)
```

Similarly, `extract_field_value('wavelnth')` gives us the wavelength value from the FITS header.

## 4. Submit the resources to the SVO

We now have everything needed to build the resources.

The final step is to create the `Provider`, give it our SVO authentication information, and tell it which FITS files to process.

The SVO API requires:

* your username, which is your email address;
* your API key.

The API key can be found or changed through your [SVO account](https://solarnet.oma.be/service/account/update).

**Do not put your real API key directly into a script that you intend to share or put under version control.**

A safer approach is to write the authentication information to a separate file containing the username and API key, separated by a colon.

For example, create the file `/home/myself/svo_auth` with the following content:

```text
myself@myserver.edu:mySecretAPIkey
```

Make sure that this file is kept private and readable only by you.

For this first example, the script looks like this:

```python
from provider_tools import (
    DataLocationFromLocalFile,
    MetadataFromFitsHeader,
    ProviderFromLocalFitsFile,
    RESTfulApi,
)


class DataLocation(DataLocationFromLocalFile):
    # Same as before ...


class Metadata(MetadataFromFitsHeader):
    # Same as before ...


class MyProvider(ProviderFromLocalFitsFile):
    # Same as before ...

AUTH_FILE = '/home/myself/svo_auth'

if __name__ == '__main__':

    api = RESTfulApi(
        auth_file=AUTH_FILE,
        debug=False,
    )

    provider = MyProvider(
        restful_api=api,
        dataset_name='MyDataset',
    )

    fits_files = [
        '/data/2025/07/20250721_120000.fits',
        '/data/2025/07/20250721_130000.fits',
        '/data/2025/07/20250721_140000.fits',
    ]

    provider.process_items(fits_files, submit=True)
```

The important line is:

```python
provider.process_items(fits_files, submit=True)
```

The provider will:

1. read each FITS file;
2. extract the required information;
3. build the metadata resource;
4. build the data_location resource;
5. submit the resources to the SVO server.

The generated resources are also printed as JSON to standard output.

The `submit=True` argument tells the provider to actually send the resources to the SVO server. You can set it to `False` if you just want to see the resources that would be sent.

## 5. Make the script operational

Our script now works, but there are still two things that would make it inconvenient to use in practice:

* we have no information about what the script is doing while it runs;
* every time we want to process different FITS files, we have to modify the Python script.

Let's fix both of these.

### Add logging

First, we can add Python's standard `logging` module. This allows the provider to give us information about what it is doing while it processes the files.

Add:

```python
import logging
```

and, before creating the API and provider, configure logging:

```python
logging.basicConfig(level=logging.INFO)
```

You can use `logging.DEBUG` instead of `logging.INFO` if you need more detailed information while troubleshooting.

We can then pass `logging` to the provider:

```python
provider = MyProvider(
    restful_api=api,
    dataset_name='MyDataset',
    logger=logging,
)
```

### Get the FITS filenames from the command line

The other problem is that we currently have to edit the script whenever we want to process a different set of files.

Instead, we can give the filenames to the script when we run it.

Python provides the command-line arguments through `sys.argv`. The first element is the name of the script itself, so `sys.argv[1:]` contains all the arguments given to the script.

We can therefore replace:

```python
fits_files = [
    '/data/2025/07/20250721_120000.fits',
    '/data/2025/07/20250721_130000.fits',
    '/data/2025/07/20250721_140000.fits'
]
```

with:

```python
fits_files = sys.argv[1:]
```

The updated script is now:

```python
import logging
import sys

from provider_tools import (
    DataLocationFromLocalFile,
    MetadataFromFitsHeader,
    ProviderFromLocalFitsFile,
    RESTfulApi,
)


class DataLocation(DataLocationFromLocalFile):
    # Same as before ...


class Metadata(MetadataFromFitsHeader):
    # Same as before ...


class MyProvider(ProviderFromLocalFitsFile):
    # Same as before ...


AUTH_FILE = '/home/myself/svo_auth'


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    api = RESTfulApi(
        auth_file=AUTH_FILE,
        debug=False,
    )

    provider = MyProvider(
        restful_api=api,
        dataset_name='MyDataset',
        logger=logging,
    )

    fits_files = sys.argv[1:]

    provider.process_items(fits_files, submit=True)
```

We can now run the script by giving it the FITS files to process:

```bash
python myscript.py \
    /data/2025/07/20250721_120000.fits \
    /data/2025/07/20250721_130000.fits \
    /data/2025/07/20250721_140000.fits
```

We can also use shell patterns to process several files at once. For example:

```bash
python submit.py /data/2025/07/*.fits
```

The shell will expand `*.fits` into the list of matching files, and `sys.argv[1:]` will contain that list.

## Other ways of providing your data

The example above uses `ProviderFromLocalFitsFile`, because having FITS files on the local computer or server is a common situation.

The package also supports other situations:

### FITS files available through URLs

If the FITS files are not stored locally but can be accessed through URLs, you can use:

```text
ProviderFromFitsUrl
```

together with:

```text
DataLocationFromUrl
```

### Data available through a TAP service

If the information about your data is available through a TAP service, you can use:

```text
ProviderFromTapRecord
```

together with:

```text
DataLocationFromTapRecord
```

and:

```text
MetadataFromTapRecord
```

### A different situation

If none of these providers matches your situation, you can create your own provider by subclassing the base `Provider` class.

You then define how your provider creates the resources by implementing its `get_resource_data()` method. The method should return a dictionary containing the metadata fields, including a `data_location` key that contains itself a dictionary with the data_location fields.

The minimal structure of the resource data would look like so:

```python
{
    "oid": str,
    "date_beg": datetime.datetime,
    "date_end": datetime.datetime,
    "wavemin": float,
    "wavemax": float,
    "data_location": {
        "file_url": str,
        "file_size": int,
        "file_path": str,
        "thumbnail_url" : str,
        "offline": bool,
    },
}
```

## Where to go from here

The example above should be enough to get you started with a typical collection of local FITS files.

If your situation is different, you can adapt the same approach to your needs.

You can also look at the real provider scripts in the `sidc_oma_be` directory. These examples show how the package is used for actual datasets.

For more detailed information about individual classes and methods, see the [Code Reference](code_reference/overview.md).
