using System.Collections.Generic;
using System.IO;
using System.Xml;
using System.Xml.Serialization;
using Serilog;

namespace DataHarvester.ctg
{
    class CTGController
    {
        ILogger _logger;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer _storage_repo;
        CTGProcessor _processor;
        Source _source;
        int _harvest_type_id;
        int _harvest_id;

        public CTGController(ILogger logger, IMonitorDataLayer mon_repo, IStorageDataLayer storage_repo,
                             Source source, int harvest_type_id, int harvest_id, CTGProcessor processor)
        {
            _logger = logger;
            _mon_repo = mon_repo;
            _storage_repo = storage_repo;
            _processor = processor;
            _source = source;
            _harvest_type_id = harvest_type_id;
            _harvest_id = harvest_id;
        }
 
        public int? LoopThroughFiles()
        {
            // Loop through the available records a chunk at a time (may be 1 for smaller record sources)
            // First get the total number of records in the system for this source
            // Set up the outer limit and get the relevant records for each pass

            int total_amount = _mon_repo.FetchFileRecordsCount(_source.id, _source.source_type, _harvest_type_id);
            int chunk = _source.harvest_chunk;
            int k = 0;
            for (int m = 0; m < total_amount; m += chunk)
            {
                // if (k > 50) break; // for testing...

                IEnumerable<StudyFileRecord> file_list = _mon_repo
                        .FetchStudyFileRecordsByOffset(_source.id, m, chunk, _harvest_type_id);

                int n = 0; string filePath = "";
                foreach (StudyFileRecord rec in file_list)
                {
                    // if (k > 50) break; // for testing...

                    n++; k++;
                    filePath = rec.local_path;

                    if (File.Exists(filePath))
                    {
                        XmlDocument xdoc = new XmlDocument();
                        xdoc.Load(filePath);
                        Study s = _processor.ProcessData(xdoc, rec.last_downloaded);

                        // store the data in the database
                        _processor.StoreData(s, _source.db_conn);

                        // update file record with last processed datetime
                        // (if not in test mode)
                        if(_harvest_type_id != 3) 
                        {
                            _mon_repo.UpdateFileRecLastHarvested(rec.id, _source.source_type, _harvest_id);
                        }
                    }
                    
                    if (k % chunk == 0) _logger.Information("Records harvested: " + k.ToString());
                }
            }

            return k;
        }
    }
}
