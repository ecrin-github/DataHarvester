using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataHarvester.euctr
{
	class EUCTRController
	{

		DataLayer common_repo;
		LoggingDataLayer logging_repo;
		EUCTRProcessor processor;
		Source source;

		public EUCTRController(Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo)
		{
			source = _source;
			processor = new EUCTRProcessor();
			common_repo = _common_repo;
			logging_repo = _logging_repo;
		}

		public async Task LoopThroughFilesAsync()
		{
     		// Construct a list of the files 
			// Rather than using a file base, it is possible
			// to use the sf records to get a list of files
			// and local paths...

			IEnumerable<FileRecord> file_list = logging_repo.FetchStudyFileRecords(source.id);
			int n = 0; string filePath = "";
			foreach (FileRecord rec in file_list)
			{
				n++;
				// for testing...
				// if (n == 500) break;

				filePath = rec.local_path;
				if (File.Exists(filePath))
				{
					string inputString = "";
					using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
					{
						inputString += streamReader.ReadToEnd();
					}

					XmlSerializer serializer = new XmlSerializer(typeof(EUCTR_Record));
					StringReader rdr = new StringReader(inputString);
					EUCTR_Record studyRegEntry = (EUCTR_Record)serializer.Deserialize(rdr);

					// break up the file into relevant data classes
					Study s = await processor.ProcessDataAsync(studyRegEntry, rec.download_datetime, common_repo);

					// check and store data object links - just pdfs for now
					// (commented out for the moment to save time during extraction).
					await HtmlHelpers.CheckURLsAsync(s.object_instances);

					// store the data in the database
					processor.StoreData(common_repo, s); 

				}

				if (n % 10 == 0) Console.WriteLine(n.ToString());
			}
		}
	}
}
