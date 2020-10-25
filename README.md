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
will update the organisation, topic and other context related data for BioLincc and Yoda sd data, but will not re-harvest trhat data first.<br/><br/>

### Overview
Unless the -G flag has been applied the initial stage is to recreate the sd tables, to receive the harvested data. After that
* The program selects the relevant source data records from the monitor database - depending on the t parameter. For t1 all records are selected, though for the larger sources this is done in batches, for t2 the date time of the last import process - also available from the monitor database - is used to compare against the datetime of the XML file's download / production. Only files downloaded or created since the last import are harvested.
* The program then loops through each of the source XML files, using the local path value in the source data records to locate the file.
* The xml file is deserialised into a C# object for further processing. The processing that follows is very different for different sources, although all the sources that are derived from WHO ICTRP files are processed in a very similar way. 
* Cleaning of common data / formatting errors occurs as part of the harvesting process, for example the removal of values that simply say 'none' or 'nil' for common study variables, or the extraction of other registry ids froom the strings in which they can be buried using regular expression technology).
* In each case the requirement is to end up with the data that is compatible with the ECRIN metadata schema. For some sources the harvest step is the second part of this conversion process, the first taking place during file generation. For others, where data are downloaded as pre-formed XML files from the source - at the moment ClinicaTrials.gov and PubMed - all the conversion process takes place in the harvesting step.
* For PubMed data in particular substantial additional processing is necessary. For instance the source data contains information about the journal but not the publisher of the article - that has to be obtained from separate lookup processes using contextual databases, that take place after the main harvesting process, using pissn and eissn numbers in the PubMNed files.
* After the harvest has created the session data in the sd tables, it is necessary to update organisation data in various tables. This uses an internal database of organisations to try and identify as many as possible of the cited organisations in a standardised form - i.e. with a default name and a system id. Without this step the organisation data becomes difficult to interrogate, as the same organiation can be present in the system in so many different forms.
* Topic data is also standardised as much as possible, with the application of MESH codes (to complement the MESH coding that exists inthe source data) to as many topic terms as possible.
* Once all data has been harvested and updated, the final steps involve the creation of md5 hashes for the key fields in the sd tables. For attribute records, for both studies and data objects, these hashes are trhen 'rolled up' to form a single hash for each set of attribute data, and then finally all the hashes for a single study or data object are rolled up into a single 'full hash' for that entity. These hash values play a critical role in identifying edited data during the data import process (see the MDR wiki for more details.

### Logging
A harvest event record is created for every harvest for each source, in the monitor database. This summarises the numbers of records harvested. In addition each individual source record is updated with the date-time they were last harvested.

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

