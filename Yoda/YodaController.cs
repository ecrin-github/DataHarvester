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
		YodaCopyHelpers yoda_mappings;

		int harvest_type_id;
		int source_id;

		public YodaController(DataLayer _common_repo, int _harvest_type_id, int _source_id)
		{
			repo = new YodaDataLayer();
			processor = new YodaProcessor();
			yoda_mappings = new YodaCopyHelpers();
			common_repo = _common_repo;
			harvest_type_id = _harvest_type_id;
			source_id = _source_id;
		}

		public void LoopThroughFiles()
		{
			// at the moment, for Yoda and BioLincc, harvest_type_id always 1
			// i.e. examine all files and transfer them to the sd tables 

			// Get the folder base from the appsettings file
			// and construct a list of the files 
			// N.B. (only one folder for all files) 

			IEnumerable<string> file_list = repo.FetchFilePaths(source_id);
			int n = 0;
			foreach (string filePath in file_list)
			{
				n++;
				// for testing...
				if (n == 50) break;

				if (File.Exists(filePath))
				{
					string inputString = "";
					using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
					{
						inputString += streamReader.ReadToEnd();
					}

					XmlSerializer serializer = new XmlSerializer(typeof(Yoda_Record));
					StringReader rdr = new StringReader(inputString);
					Yoda_Record studyRegEntry = (Yoda_Record)serializer.Deserialize(rdr);

					// break up the file into relevant data classes
					Study s = processor.ProcessData(repo, studyRegEntry);

					// store the data in the database			
					processor.StoreData(yoda_mappings, repo, s);  // FOR NOW!!!!!

				}

				if (n % 100 == 0) Console.WriteLine(n.ToString());
			}
		}
	}

}
