using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataHarvester.isrctn
{
	class ISRCTNController
	{
		DataLayer common_repo;
		LoggingDataLayer logging_repo;
		ISRCTNProcessor processor;
		int source_id;

		public ISRCTNController(int _source_id, DataLayer _common_repo, LoggingDataLayer _logging_repo)
		{
			source_id = _source_id;
			processor = new ISRCTNProcessor();
			common_repo = _common_repo;
			logging_repo = _logging_repo;
		}

		public async Task LoopThroughFilesAsync()
		{
			URLChecker checker = new URLChecker();

			// Get the folder base from the appsettings file
			// and construct a list of the files 
			// N.B. (only one folder for all files) 

			// Construct a list of the files 
			// Rather than using a file base, it is possible
			// to use the sf records to get a list of files
			// and local paths...

			IEnumerable<FileRecord> file_list = logging_repo.FetchStudyFileRecords(source_id);
			int n = 0; string filePath = "";
			foreach (FileRecord rec in file_list)
			{
				n++;
				// for testing...
				//if (f == 5) break;

				filePath = rec.local_path;
				if (File.Exists(filePath))
				{
					string inputString = "";
					using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
					{
						inputString += streamReader.ReadToEnd();
					}

					XmlSerializer serializer = new XmlSerializer(typeof(ISCTRN_Record));
					StringReader rdr = new StringReader(inputString);
					ISCTRN_Record studyRegEntry = (ISCTRN_Record)serializer.Deserialize(rdr);

					// break up the file into relevant data classes
					Study s = await processor.ProcessDataAsync(studyRegEntry, rec.download_datetime, common_repo);

					// store the data in the database
					processor.StoreData(common_repo, s); 
				}

				if (n % 100 == 0) Console.WriteLine(n.ToString());
			}
		}
	}
}
