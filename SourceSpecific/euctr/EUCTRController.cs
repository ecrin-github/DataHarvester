using System;
using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;
using Serilog;

namespace DataHarvester.euctr
{
    class EUCTRController
	{
		ILogger _logger;
		IMonitorDataLayer _mon_repo;
		IStorageDataLayer storage_repo;
		EUCTRProcessor processor;
		Source source;
		int harvest_type_id;
		int harvest_id;

		public EUCTRController(ILogger logger, IMonitorDataLayer mon_repo, IStorageDataLayer _storage_repo,
							   Source _source, int _harvest_type_id, int _harvest_id)
		{
			_logger = logger;
			_mon_repo = mon_repo;
			storage_repo = _storage_repo;
			processor = new EUCTRProcessor();
			source = _source;
			harvest_type_id = _harvest_type_id;
			harvest_id = _harvest_id;
		}

		public int? LoopThroughFiles()
		{
     		// Construct a list of the files 
			// Rather than using a file base, it is possible
			// to use the sf records to get a list of files
			// and local paths...

			IEnumerable<StudyFileRecord> file_list = _mon_repo.FetchStudyFileRecords(source.id, harvest_type_id);
			int n = 0; string filePath = "";
			foreach (StudyFileRecord rec in file_list)
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

						// at least one file has this odd ('start of text') character, 
						// which throws an error in the deserialisation process
						inputString = inputString.Replace("&#x2;", "");
					}

					try
					{
						XmlSerializer serializer = new XmlSerializer(typeof(EUCTR_Record));
						StringReader rdr = new StringReader(inputString);
						EUCTR_Record studyRegEntry = (EUCTR_Record)serializer.Deserialize(rdr);

						// break up the file into relevant data classes
						Study s = processor.ProcessData(studyRegEntry, rec.last_downloaded, storage_repo, _mon_repo, _logger);

						// check and store data object links - just pdfs for now
						// (commented out for the moment to save time during extraction).
						// await HtmlHelpers.CheckURLsAsync(s.object_instances);

						// store the data in the database
						processor.StoreData(storage_repo, s, _mon_repo);

						// update file record with last processed datetime
						_mon_repo.UpdateFileRecLastHarvested(rec.id, "study", harvest_id);
					}

					catch (Exception e)
					{
						_logger.Error("In main processing loop, record number " + n.ToString() + ": " + e.Message);
					}

				}

				if (n % 10 == 0) _logger.Information(n.ToString());
			}
			return n;
		}
	}
}
