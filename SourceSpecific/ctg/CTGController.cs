using Microsoft.VisualBasic.CompilerServices;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataHarvester.ctg
{
	class CTGController
	{
		DataLayer common_repo;
		LoggingDataLayer logging_repo;
		CTGProcessor processor;
		Source source;
		int harvest_type_id;
		DateTime? cutoff_date;
		int last_harvest_id;

		public CTGController(int _last_harvest_id, Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo, int _harvest_type_id, DateTime? _cutoff_date)
		{
			source = _source;
			processor = new CTGProcessor();
			common_repo = _common_repo;
			logging_repo = _logging_repo;
			harvest_type_id = _harvest_type_id;
			cutoff_date = _cutoff_date;
			last_harvest_id = _last_harvest_id;
		}

		public int? LoopThroughFiles()
		{
			// Loop through the available records 1000 at a time
			// First get the total number of records in the system for this source

			// Set up the outer limit and get the relevant records for each pass
			int total_amount = logging_repo.FetchFileRecordsCount(source.id, "study", harvest_type_id, cutoff_date);
			int chunk = 1000;
			int k = 0;
			for (int m = 0; m < total_amount; m += chunk)
			{
				IEnumerable<StudyFileRecord> file_list = logging_repo.FetchStudyFileRecordsByOffset(source.id, m, chunk, harvest_type_id, cutoff_date);

				int n = 0; string filePath = "";
				foreach (StudyFileRecord rec in file_list)
				{
					n++; k++;
					filePath = rec.local_path;

					if (File.Exists(filePath))
					{
						string inputString = "";
						using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
						{
							inputString += streamReader.ReadToEnd();
						}

						XmlSerializer serializer = new XmlSerializer(typeof(FullStudy));
						StringReader rdr = new StringReader(inputString);
						FullStudy studyRegEntry = (FullStudy)serializer.Deserialize(rdr);

						// break up the file into relevant data classes
						Study s = processor.ProcessData(studyRegEntry, rec.last_downloaded, common_repo);

						// check and store data object links - just pdfs for now
						// (commented out for the moment to save time during extraction).
						// await HtmlHelpers.CheckURLsAsync(s.object_instances);

						// store the data in the database
						processor.StoreData(common_repo, s);

						// update file record with last processed datetime
						logging_repo.UpdateFileRecLastHarvested(rec.id, "study", last_harvest_id);

					}
					
					if (k % 100 == 0) StringHelpers.SendFeedback(m.ToString() + ": " + n.ToString());
				}
			}

            return (k);
		}

		
	}
}
