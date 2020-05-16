using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;

namespace DataHarvester.Yoda
{
    
	class YodaController
	{
		DataLayer common_repo; 
		YodaDataLayer repo;
		YodaProcessor processor;
		int harvest_type_id;
		int source_id;

		public YodaController(DataLayer _common_repo, int _harvest_type_id, int _source_id)
		{
			source_id = _source_id;
			repo = new YodaDataLayer(source_id);
			processor = new YodaProcessor();
			common_repo = _common_repo;
			harvest_type_id = _harvest_type_id;
		}

		public void EstablishNewSDTables()
		{
			repo.DeleteSDStudyTables();
			repo.DeleteSDObjectTables();
			repo.BuildNewSDStudyTables();
			repo.BuildNewSDObjectTables();
		}

		public void LoopThroughFiles()
		{
			// at the moment, for Yoda and BioLincc, harvest_type_id always 1
			// i.e. examine all files and transfer them to the sd tables 

			// Get the folder base from the appsettings file
			// and construct a list of the files 
			// N.B. (only one folder for all files) 

			IEnumerable<FileRecord> file_list = common_repo.FetchStudyFileRecords(source_id);
			int n = 0; string filePath = "";
			foreach (FileRecord rec in file_list)
			{
				n++;
				// for testing...
				//if (n == 50) break;

				filePath = rec.local_path;
				if (File.Exists(filePath))
				{
					string inputString = "";
					using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
					{
						inputString += streamReader.ReadToEnd();
					}

					XmlSerializer serializer = new XmlSerializer(typeof(YodaRecord));
					StringReader rdr = new StringReader(inputString);
					YodaRecord studyRegEntry = (YodaRecord)serializer.Deserialize(rdr);

					// break up the file into relevant data classes
					Study s = processor.ProcessData(repo, studyRegEntry, rec.download_datetime);

					// store the data in the database			
					processor.StoreData(repo, s);  // FOR NOW!!!!!

					// update file record with last processed datetime
					common_repo.UpdateStudyFileRecLastProcessed(rec.id);

				}

				if (n % 10 == 0) Console.WriteLine(n.ToString());
			}
		}

		public void UpdateIds()
		{
			repo.UpdateStudyIdentifierOrgs();
			repo.UpdateDataObjectOrgs();

			// also add in default language data
			repo.StoreObjectLanguages();
		}

		public void InsertHashes()
		{
			repo.CreateStudyHashes();
			repo.CreateStudyCompositeHashes();
			repo.CreateDataObjectHashes();
			repo.CreateObjectCompositeHashes();
		}
	}

}
