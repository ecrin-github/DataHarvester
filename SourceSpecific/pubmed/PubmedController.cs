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
            processor = new PubmedProcessor(storage_repo, mon_repo, logger);
            source = _source;
            harvest_type_id = _harvest_type_id;
            harvest_id = _harvest_id; 
        }

        public int? LoopThroughFiles()
        {
            // Loop through the available records a chunk at a time (may be 1 for smaller rexord sources)
            // First get the total number of records in the system for this source
            // Set up the outer limit and get the relevant records for each pass            //string fileBase = source.local_folder;

            int total_amount = _mon_repo.FetchFileRecordsCount(source.id, "object", harvest_type_id);
            int chunk = 1000;
            int k = 0;
            for (int m = 0; m < total_amount; m += chunk)
            {
                IEnumerable<ObjectFileRecord> file_list = _mon_repo
                    .FetchObjectFileRecordsByOffset(source.id, m, chunk, harvest_type_id);

                int n = 0; string filePath = "";
                foreach (ObjectFileRecord rec in file_list)
                {
                    n++; k++;
                    filePath = rec.local_path;

                    if (File.Exists(filePath))
                    {
                        XmlDocument xdoc = new XmlDocument();
                        xdoc.Load(filePath);
                        CitationObject c = processor.ProcessData(xdoc, rec.last_downloaded);
                        processor.StoreData(c, source.db_conn);

                        /// update file record with last processed datetime
                        // (if not in test mode)
                        if (harvest_type_id != 3)
                        {
                            _mon_repo.UpdateFileRecLastHarvested(rec.id, "object", harvest_id);
                        }
                    }

                    if (k % 100 == 0) _logger.Information(k.ToString());
                }

                // if (k > 9990) break;  // testing only
            }

            return k;
        }

        public void DoPubMedPostProcessing()
        {
            ContextDataManager.Source context_source = new ContextDataManager.Source(source.id, source.preference_rating, source.database_name, source.db_conn, 
                                                              source.has_study_tables, source.has_study_topics, source.has_study_contributors);
            ContextDataManager.PubmedPostProcBuilder ppb = new ContextDataManager.PubmedPostProcBuilder(context_source);
            Credentials creds = _mon_repo.Credentials;
            ContextDataManager.Credentials context_creds = new ContextDataManager.Credentials(creds.Host, creds.Username, creds.Password);
            ppb.EstablishContextForeignTables(context_creds);

            ppb.ObtainPublisherNames();
            ppb.UpdatePublisherOrgIds();
            ppb.UpdateIdentifierPublisherData();
            ppb.CreateDataObjectsTable();
            ppb.CreateTotalLinksTable();

            ppb.DropContextForeignTables();
        }

    }
}
