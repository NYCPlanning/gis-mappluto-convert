# MapPLUTO ESRI Conversion

*******************************

This script converts the MapPLUTO data set into an ESRI Feature Class format from a static CSV and shapefile provided by Data Engineering’s PostGIS data process.

### Prerequisites

A version of Python 2/3 with the default ArcPy installation that comes with ArcGIS Desktop or ArcGIS Pro and the following packages. 

##### MapPLUTOCSV2FC_Conversion.py

```
arcpy, os, pandas, timeit, shutil, datetime, configparser, sys, traceback
```

### Instructions for running

##### MapPLUTOCSV2FC_Conversion.py

1. Open the script in any integrated development environment (PyCharm is suggested)

2. Ensure that your IDE is set to be utilizing a version of Python 3 as its interpreter for this script or a version of Python 2 which has had pandas installed via pip.

3. Ensure that the configuration ini file is up-to-date.

4. Run the script. It will create temporary file geodatabases for both MapPLUTO clipped and unclipped in your temporary directory
