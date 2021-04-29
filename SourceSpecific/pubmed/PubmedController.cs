using System.Collections.Generic;
using System.IO;
using System.Xml;
using Serilog;

namespace DataHarvester.pubmed
{
    class PubmedController
    {
        ILogger _logger;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer storage_repo;
        PubmedProcessor processor;
        Source source;
        int harvest_type_id;
        int harvest_id; 

        public PubmedController(ILogger logger, IMonitorDataLayer mon_repo, IStorageDataLayer _storage_repo,
                             Source _source, int _harvest_type_id, int _harvest_id)
        {
            _logger = logger;
            _mon_repo = mon_repo;
            storage_repo = _storage_repo;
            processor = new PubmedProcessor();
            source = _source;
            harvest_type_id = _harvest_type_id;
            harvest_id = _harvest_id; 
        }


        public int? LoopThroughFiles()
        {
            string fileBase = source.local_folder;

            int total_amount = _mon_repo.FetchFileRecordsCount(source.id, "object", harvest_type_id);
            int chunk = 1000;
            int k = 0;

            for (int m = 0; m < total_amount; m += chunk)
            {
                IEnumerable<ObjectFileRecord> file_list = _mon_repo.FetchObjectFileRecordsByOffset(source.id, m, chunk, harvest_type_id);
                int n = 0; string filePath = "";
                foreach (ObjectFileRecord rec in file_list)
                {
                    n++; k++;
                    filePath = rec.local_path;
                    if (File.Exists(filePath))
                    {
                        XmlDocument xdoc = new XmlDocument();
                        xdoc.Load(filePath);
                        CitationObject c = processor.ProcessData(rec.sd_id, xdoc, rec.last_downloaded, harvest_id, _mon_repo, _logger);
                        processor.StoreData(storage_repo, c, _mon_repo);

                        // update file record with last processed datetime
                        _mon_repo.UpdateFileRecLastHarvested(rec.id, "object", harvest_id);
                    }

                    if (k % 100 == 0) _logger.Information(k.ToString());
                }

                // if (k > 9990) break;  // testing only
            }

            return k;
        }

        public void DoPubMedPostProcessing()
        {
            PubmedPostProcBuilder ppb = new PubmedPostProcBuilder(storage_repo.ConnString, source);
            ppb.EstablishContextForeignTables(storage_repo.Credentials.Username, storage_repo.Credentials.Password);

            ppb.ObtainPublisherNames();
            ppb.UpdatePublisherOrgIds();
            ppb.UpdateIdentifierPublisherData();
            ppb.CreateDataObjectsTable();
            ppb.CreateTotalLinksTable();

            ppb.DropContextForeignTables();
        }

    }
}
