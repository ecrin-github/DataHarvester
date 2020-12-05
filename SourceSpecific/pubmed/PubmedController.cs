using System.Collections.Generic;
using System.IO;
using System.Xml;

namespace DataHarvester.pubmed
{
    class PubmedController
    {
        DataLayer repo;
        LoggingDataLayer logging_repo;
        PubmedProcessor processor;
        Source source;
        int harvest_id;
        int harvest_type_id;

        public PubmedController(int _harvest_id, Source _source, DataLayer _repo, LoggingDataLayer _logging_repo, int _harvest_type_id)
        {
            source = _source;
            processor = new PubmedProcessor();
            repo = _repo;
            logging_repo = _logging_repo;
            harvest_id = _harvest_id;
            harvest_type_id = _harvest_type_id;
        }


        public int? LoopThroughFiles()
        {
            string fileBase = source.local_folder;

            int total_amount = logging_repo.FetchFileRecordsCount(source.id, "object", harvest_type_id);
            int chunk = 1000;
            int k = 0;

            for (int m = 0; m < total_amount; m += chunk)
            {
                IEnumerable<ObjectFileRecord> file_list = logging_repo.FetchObjectFileRecordsByOffset(source.id, m, chunk, harvest_type_id);
                int n = 0; string filePath = "";
                foreach (ObjectFileRecord rec in file_list)
                {
                    n++; k++;
                    filePath = rec.local_path;
                    if (File.Exists(filePath))
                    {
                        XmlDocument xdoc = new XmlDocument();
                        xdoc.Load(filePath);
                        CitationObject c = processor.ProcessData(rec.sd_id, xdoc, rec.last_downloaded, harvest_id, logging_repo);
                        processor.StoreData(repo, c, logging_repo);

                        // update file record with last processed datetime
                        logging_repo.UpdateFileRecLastHarvested(rec.id, "object", harvest_id);
                    }

                    if (k % 100 == 0) logging_repo.LogLine(k.ToString());
                }

                // if (k > 9990) break;  // testing only
            }

            return k;
        }

        public void DoPubMedPostProcessing()
        {
            PubmedPostProcBuilder ppb = new PubmedPostProcBuilder(repo.ConnString, source);
            ppb.EstablishContextForeignTables(repo.Username, repo.Password);

            ppb.ObtainPublisherNames();
            ppb.UpdatePublisherOrgIds();
            ppb.UpdateIdentifierPublisherData();
            ppb.CreateDataObjectsTable();
            ppb.CreateTotalLinksTable();

            ppb.DropContextForeignTables();
        }

    }
}
