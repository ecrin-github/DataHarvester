# DataHarvester
Takes downloaded source files and loads their data into the mdr's source databases.

The program uses the XML files already downloaded for a data into a source folder (one is designated for each source). The XML files, or a subset as controlled by the parameters - see below - are converted into data in the 'sd' schema (= session data) tables within each source database. Note that on each run the sd tables are dropped and created anew, and thus only ever contain the data from the most recent harvest. The tables present will vary in different databases, though if a table *is* present it will have a consistent structure in every database. The conversion to sd data therefore represents the second and final stage of the conversion of the source data into the consistent ECRIN schema. For that reason the detailed code for different sources can vary widely. <br/><br/>
The program represents the second stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => **Harvest** => Import => Aggregation<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>

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

