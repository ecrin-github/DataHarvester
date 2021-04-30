using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;
using Serilog;

namespace DataHarvester.yoda
{
    class YodaController
    {
        ILogger _logger;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer storage_repo;
        YodaProcessor processor;
        Source source;
        int harvest_type_id;
        int harvest_id;

        public YodaController(ILogger logger, IMonitorDataLayer mon_repo, IStorageDataLayer _storage_repo,
                              Source _source, int _harvest_type_id, int _harvest_id)
        { 
            _logger = logger;
            _mon_repo = mon_repo;
            storage_repo = _storage_repo;
            processor = new YodaProcessor(storage_repo, mon_repo, logger);
            source = _source;
            harvest_type_id = _harvest_type_id;
            harvest_id = _harvest_id;

        }

        public int? LoopThroughFiles()
        {
            // Rather than using a file base, it is possible
            // to use the sf records to get a list of files
            // and local paths...

            IEnumerable<StudyFileRecord> file_list = _mon_repo.FetchStudyFileRecords(source.id);
            int n = 0; string filePath = "";
            foreach (StudyFileRecord rec in file_list)
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

                    XmlSerializer serializer = new XmlSerializer(typeof(Yoda_Record));
                    StringReader rdr = new StringReader(inputString);
                    Yoda_Record studyRegEntry = (Yoda_Record)serializer.Deserialize(rdr);

                    // break up the file into relevant data classes
                    Study s = processor.ProcessData(studyRegEntry, rec.last_downloaded);

                    // store the data in the database			
                    processor.StoreData(s, source.db_conn);

                    // update file record with last processed datetime
                    // (if not in test mode)
                    if (harvest_type_id != 3)
                    {
                        _mon_repo.UpdateFileRecLastHarvested(rec.id, "study", harvest_id);
                    }
                }

                if (n % 100 == 0) _logger.Information(n.ToString());
            }

            return n;
        }
    }
}
