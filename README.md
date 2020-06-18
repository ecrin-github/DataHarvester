# DataHarvester
Takes source files and loads data into databases

The program uses 


### Parameters
The system is currently a consiole app, and takes up to 3 parameters
* A 6 digit integer representing the source (e.g. 100120 is Clinical Trials,gov)
* A single digit integer representing the harvest type (see listing below). If not provided the default harvest type will be read from the database.
* A cut-off date for those harvest types that are date dependent. In such cases only files that have been revised since the cutoff data provided will be harvested into the sd tables.

The plan is to wrap a UI around the app at some point.

### Harvest Types
1: Harvest all
*All files in the source data foilder will be converted into data in the sd tables. Used for relatively small sources and / or those that have no 'last revised date'~

2: Harvest revised since (cutoff date)
**

3: Harvest those considered not completed
**


### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

