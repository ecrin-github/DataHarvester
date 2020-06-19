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

        public PubmedController(Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo)
        {
            source = _source;
            processor = new PubmedProcessor();
            common_repo = _common_repo;
            logging_repo = _logging_repo;
            pubmed_repo = new PubmedDataLayer();
        }


        async public Task LoopThroughFilesAsync()
        {
            CopyHelpers helpers = new CopyHelpers();
            string fileBase = source.local_folder;
            
            // Put all the PMIDs of interest in memory and use to find the source 
            // XML file of each (as these files have previously been stored using 
            // the PMID as an integral part of the file name.

            int total_number = (int)pubmed_repo.Get_pmid_record_count();
            int i = 0;
            int number_of_loops_needed = (total_number / ((int) source.grouping_range_by_id)) + 1;


            for (int loop = 0; loop < number_of_loops_needed; loop++)
            {
                //if (loop > 1) break; // when testing...

                IEnumerable<ObjectFileRecord> file_records = pubmed_repo.Get_pmid_records(loop);

                foreach (ObjectFileRecord fe in file_records)
                {
                    //if (i > 1000) break; // when testing...

                    string pmid = fe.sd_id;
                    string fileName = fe.local_path; 

                    if (File.Exists(fileName))
                    {
                        // Get file with that id in its name - load as an XML document and 
                        // send for processing, then store the data in the constructed Data object.

                        // First check not already in the database...
                        // Indicate if it is ... (at least for now - not if updating!)
                        if (!pubmed_repo.FileInDatabase(pmid))
                        {
                            XmlDocument xdoc = new XmlDocument();
                            xdoc.Load(fileName);

                            CitationObject c = processor.ProcessData(common_repo, pmid, xdoc);
                            processor.StoreData(common_repo, c);
                            i++;

                            pubmed_repo.UpdateFileRecord(fe.id);
                        }
                    }
                    else
                    {
                        pubmed_repo.StoreExtractionNote(pmid, 12, "No file found for this PMID (pmid = : " + pmid.ToString() + ")");
                    }

                    if (i % 100 == 0) Console.WriteLine(i.ToString());
                }
            }
         
        }
    }
}
