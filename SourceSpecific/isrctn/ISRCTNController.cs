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
		Source source;
		int harvest_type_id;
		int last_harvest_id;

		public ISRCTNController(int _last_harvest_id, Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo, int _harvest_type_id)
		{
			source = _source;
			processor = new ISRCTNProcessor();
			common_repo = _common_repo;
			logging_repo = _logging_repo;
			harvest_type_id = _harvest_type_id;
			last_harvest_id = _last_harvest_id;
		}

		public async Task<int?> LoopThroughFilesAsync()
		{
			// Get the folder base from the appsettings file
			// and construct a list of the files 
			// N.B. (only one folder for all files) 

			// Construct a list of the files 
			// Rather than using a file base, it is possible
			// to use the sf records to get a list of files
			// and local paths...

			IEnumerable<StudyFileRecord> file_list = logging_repo.FetchStudyFileRecords(source.id, harvest_type_id);
			int n = 0; string filePath = "";
			foreach (StudyFileRecord rec in file_list)
			{
				n++;
				// for testing...
				// if (n == 100) break;

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
					Study s = await processor.ProcessDataAsync(studyRegEntry, rec.last_downloaded, common_repo);

					// store the data in the database
					processor.StoreData(common_repo, s);

					// update file record with last processed datetime
					logging_repo.UpdateFileRecLastHarvested(rec.id, "study", last_harvest_id);
				}

				if (n % 10 == 0) StringHelpers.SendFeedback(n.ToString());
			}

			return n;
		}
	}
}
