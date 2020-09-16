using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Xml;

namespace DataHarvester.pubmed
{
    class PubmedController
    {
        DataLayer common_repo;
        LoggingDataLayer logging_repo;
        PubmedDataLayer pubmed_repo;
        PubmedProcessor processor;
        Source source;
        int last_harvest_id;
        int harvest_type_id;
        DateTime? cutoff_date;

        public PubmedController(int _last_harvest_id, Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo, int _harvest_type_id, DateTime? _cutoff_date)
        {
            source = _source;
            processor = new PubmedProcessor();
            common_repo = _common_repo;
            logging_repo = _logging_repo;
            pubmed_repo = new PubmedDataLayer();
            last_harvest_id = _last_harvest_id;
            harvest_type_id = _harvest_type_id;
            cutoff_date = _cutoff_date;
        }


        public int? LoopThroughFiles()
        {
            // ***************************************************
            // set up pp local extraction errors folder .....
            // or better to use centtral sf mon....
            // **************************************************

            string fileBase = source.local_folder;

            // harvest_type_id can be 1 (all), 2 (use cutoff date) or 3 (harvest_type_id only 'incomplete' files)
            int total_amount = logging_repo.FetchFileRecordsCount(source.id, "object", harvest_type_id, cutoff_date);
            int chunk = 1000;
            int k = 0;

            for (int m = 0; m < total_amount; m += chunk)
            {
                IEnumerable<ObjectFileRecord> file_list = logging_repo.FetchObjectFileRecordsByOffset(source.id, m, chunk, harvest_type_id, cutoff_date);
                int n = 0; string filePath = "";
                foreach (ObjectFileRecord rec in file_list)
                {
                    n++; k++;
                    filePath = rec.local_path;
                    if (File.Exists(filePath))
                    {
                        XmlDocument xdoc = new XmlDocument();
                        xdoc.Load(filePath);
                        // CitationObject c = 
                        processor.ProcessData(common_repo, rec.sd_id, xdoc, rec.last_downloaded);
                        //processor.StoreData(common_repo, c);

                        // update file record with last processed datetime
                        logging_repo.UpdateFileRecLastHarvested(rec.id, "object", last_harvest_id);
                    }

                    if (n % 10 == 0) Console.WriteLine(m.ToString() + ": " + n.ToString());
                }
            }
            return k;
        }

        public void IdentifyPublishers()
        {
            // to do
        }

        public void CollectTogetherStudyLinks()
        {
            // to do
        }
    }
}
