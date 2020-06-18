# DataHarvester
Takes downloaded source files and loads their data into databases for further inspection and processing.

The program uses XML files in a data source folder (one is designated for each source) - the files have been previously downloaded using the DataDownloader system. The XML files are converted into data in the 'sd' schema (= session data) tables within each MDR database. Note that on each run the sd tables are dropped and created anew, and thus only ever contain the data from the most recent harvest. The tables present will vary in different databases, though if a table *is* present it will have a consistent structure in every database. The conversion to sd data trherefore represents the second and final stage of the source data into a consistent schema. 

### Parameters
The system is currently a consiole app, and takes up to 3 parameters
* A 6 digit integer representing the source (e.g. 100120 is Clinical Trials,gov)
* A single digit integer representing the harvest type (see listing below). If not provided the default harvest type will be read from the database.
* A cut-off date for those harvest types that are date dependent. In such cases only files that have been revised since the cutoff data provided will be harvested into the sd tables.

The plan is to wrap a UI around the app at some point.

### Harvest Types
1: Harvest all<br/>
*All files in the source data folder will be converted into data in the sd tables. Used for relatively small sources and / or those that have no 'last revised date'*

2: Harvest revised since (cutoff date)<br/>
*Processes only files that have a 'last revised date' greater than the cutoff date given. Harvests of this type therefore require a third parameter to be supplied.*

3: Harvest those considered not completed<br/>
*Processes those files thast are marked as 'incomplete' in the logging system and ignores those marked as 'comnplete'. The latter designation is sometimes given to files that, whilst they do not contain a date last revised attribute, are old enough and seem to contain sufficient data that any further editing seems very unlikely. Note that even files that are 'complete', however, can ne periodically examined (e.g. on an annual basis) by over-riding the default download and harvest ssettings.*

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

