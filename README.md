SOLARNET Virtual Observatory (SVO)
==================================
The SVO is a service first supported by the [SOLARNET](http://solarnet-east.eu/) project, funded by the European Commission’s FP7 Capacities Programme under the Grant Agreement 312495. Then made operational thanks to the [SOLARNET2](https://solarnet-project.eu) project, funded by the the European Union's Horizon 2020 Research and Innovation Programme under Grant Agreement 824135.

It's purpose is to collect metadata from as many solar observations as possible, especially those made thanks to the SOLARNET projects, in a common catalog and make them available to the scientific community.

A first prototype version was released in February 2016, and the operational version is available now at https://solarnet2.oma.be

The SVO code is split in several parts:
- A [web server](https://github.com/bmampaey/SOLARNET-server)
- A [web client](https://github.com/bmampaey/SOLARNET-web-client)
- A [python client](https://github.com/bmampaey/SOLARNET-python-client)
- An [IDL client](https://github.com/bmampaey/SOLARNET-IDL-client)
- [Data provider tools](https://github.com/bmampaey/SOLARNET-provider-tools)


SOLARNET-provider-tools
=======================
Python3 tools for the data providers of the SOLARNET Virtual Observatory

The tools include :
 - A script to generate the keywords definitions for the SOLARNET Virtual Observatory by scanning a series of FITS files `extract_keywords_from_fits.py`
 - An example script for data providers to push metadata and data location records via the RESTful API `submit_record.py`

All tools have help that can be displayed by executing the script with the `--help` flag.

Installing the tools
--------------------

Download the [ZIP archive](https://github.com/bmampaey/SOLARNET-provider-tools/archive/refs/heads/main.zip) from GitHub, and unzip it.

Install the required python packages listed in requirements.txt, for example using pip :
```
pip install -r requirements.txt
```
