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
		LoggingDataLayer logging_repo;
		YodaProcessor processor;
		int source_id;

		public YodaController(int _source_id, DataLayer _common_repo, LoggingDataLayer _logging_repo)
		{
			source_id = _source_id;
			//yoda_repo = new YodaDataLayer(source_id);
			processor = new YodaProcessor();
			common_repo = _common_repo;
			logging_repo = _logging_repo;
		}

		public void LoopThroughFiles()
		{
			// at the moment, for Yoda and BioLincc, harvest_type_id always 1
			// i.e. examine all files and transfer them to the sd tables 

			// Get the folder base from the appsettings file
			// and construct a list of the files 
			// N.B. (only one folder for all files) 

			IEnumerable<FileRecord> file_list = logging_repo.FetchStudyFileRecords(source_id);
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
					Study s = processor.ProcessData(studyRegEntry, rec.download_datetime);

					// store the data in the database			
					processor.StoreData(common_repo, s);  // FOR NOW!!!!!

					// update file record with last processed datetime
					logging_repo.UpdateStudyFileRecLastProcessed(rec.id);

				}

				if (n % 10 == 0) Console.WriteLine(n.ToString());
			}
		}

		
	}

}
