# DataHarvester
Takes downloaded source files and loads their data into the mdr's source databases.

The program uses the XML files already downloaded for a data into a source folder (one is designated for each source). The XML files, or a subset as controlled by the parameters - see below - are converted into data in the 'sd' schema (= session data) tables within each source database. Note that on each run the sd tables are dropped and created anew, and thus only ever contain the data from the most recent harvest. The tables present will vary in different databases, though if a table *is* present it will have a consistent structure in every database. The conversion to sd data therefore represents the second and final stage of the conversion of the source data into the consistent ECRIN schema. For that reason the detailed code for different sources can vary widely. <br/><br/>
The program represents the second stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => **Harvest** => Import => Aggregation<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>

### Parameters
The system is currently a console app, and takes the folowing parameters:<br/>
**-s**, followed by a comma delimited string of integer ids, each represnting a source: The source systems to be harvested.<br/>
**-t**, followed by 1 or 2: indicates the type of harvesting to be carried out. If 1, the harvesting will be of all files in the source folder , repreesentging 100% of the data that has been downloaded. If 2, the harvest is only of files that have been re-downloaded (becuase they represent new or changed source data) after the datetimer of the last import process. Note that it si the last *import* event (for that source) which is important here, not the last *harvest* event. Multiple harvests between imports do not therefore affect the files that are harvested.<br/>
**-G**: is a flag that can be applied that prevents a normal harvest occuring, so that the sd tables are not recreated and reloaded. Instead they are updated using revised contextual data, so that - for example - organisation Ids and topic data codes can be re-applied. The option provides a relatively efficient way of updating data, though obviously works better if preceded with a t1 full harvest of all data. Because the data is revised the various composite hash values that summarise data content also have to be re-created.<br/><br/>
Thus, the parameter string<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-s "100120" -t2<br/>
will harvest data from source 100120 (ClinicalTrials.gov)that has been (re-)downloaded since the last import process, but the parameter string<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-s "100126" -t1<br/>
will harvest all the data from source 100126 (ISRCTN)
The parameter string<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; -s "101900, 101901" -G<br/>
will update the organisation, topic and other context related data for BioLincc and Yoda sd data.<br/><br/>

### Overview


### Logging


### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

