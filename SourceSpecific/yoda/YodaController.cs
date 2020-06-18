using System;
using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;

namespace DataHarvester.yoda
{
	class YodaController
	{
		DataLayer common_repo;
		LoggingDataLayer logging_repo;
		YodaProcessor processor;
		Source source;

		public YodaController(Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo)
		{
			source = _source;
			processor = new YodaProcessor();
			common_repo = _common_repo;
			logging_repo = _logging_repo;
		}

		public void LoopThroughFiles()
		{
			// Rather than using a file base, it is possible
			// to use the sf records to get a list of files
			// and local paths...

			IEnumerable<FileRecord> file_list = logging_repo.FetchStudyFileRecords(source.id);
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
					processor.StoreData(common_repo, s);  

					// update file record with last processed datetime
					logging_repo.UpdateStudyFileRecLastProcessed(rec.id);

				}

				if (n % 10 == 0) Console.WriteLine(n.ToString());
			}
		}

		
	}

}
