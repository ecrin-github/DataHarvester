using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;
using Serilog;

namespace DataHarvester.ctg
{
    class CTGController
    {
        ILogger _logger;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer storage_repo;
        CTGProcessor processor;
        Source source;
        int harvest_type_id;
        int harvest_id;

        public CTGController(ILogger logger, IMonitorDataLayer mon_repo, IStorageDataLayer _storage_repo,
                             Source _source, int _harvest_type_id, int _harvest_id)
        {
            _logger = logger;
            _mon_repo = mon_repo;
            storage_repo = _storage_repo;
            processor = new CTGProcessor();
            source = _source;
            harvest_type_id = _harvest_type_id;
            harvest_id = _harvest_id;
        }


        public int? LoopThroughFiles()
        {
            // Loop through the available records 1000 at a time
            // First get the total number of records in the system for this source

            // Set up the outer limit and get the relevant records for each pass
            int total_amount = _mon_repo.FetchFileRecordsCount(source.id, "study", harvest_type_id);
            int chunk = 1000;
            int k = 0;
            for (int m = 0; m < total_amount; m += chunk)
            {
                IEnumerable<StudyFileRecord> file_list = _mon_repo.FetchStudyFileRecordsByOffset(source.id, m, chunk, harvest_type_id);

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
                        Study s = processor.ProcessData(studyRegEntry, rec.last_downloaded, storage_repo, _mon_repo, _logger);

                        // check and store data object links - just pdfs for now
                        // (commented out for the moment to save time during extraction).
                        // await HtmlHelpers.CheckURLsAsync(s.object_instances);

                        // store the data in the database
                        processor.StoreData(storage_repo, s, _mon_repo);

                        // update file record with last processed datetime
                        // (if not in test mode)
                        if(harvest_type_id != 3) {
                            _mon_repo.UpdateFileRecLastHarvested(rec.id, "study", harvest_id);
                        }
                    }
                    
                    if (k % 100 == 0) _logger.Information(m.ToString() + ": " + n.ToString());
                }
            }

            return (k);
        }

        
    }
}
