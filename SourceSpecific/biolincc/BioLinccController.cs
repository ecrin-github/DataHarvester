using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;
using Serilog;

namespace DataHarvester.biolincc
{
    public class BioLinccController
    {
        ILogger _logger;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer storage_repo;
        BioLinccDataLayer biolincc_repo;
        BioLinccProcessor processor;
        BioLinccIdentifierProcessor identity_processor;
        Source source;
        int harvest_type_id;
        int harvest_id;

        public BioLinccController(ILogger logger, IMonitorDataLayer mon_repo, IStorageDataLayer _storage_repo,
                                  Source _source, int _harvest_type_id, int _harvest_id)
        {
            _logger = logger;
            _mon_repo = mon_repo; 
            storage_repo = _storage_repo;
            processor = new BioLinccProcessor(storage_repo, mon_repo, logger);
            identity_processor = new BioLinccIdentifierProcessor();
            biolincc_repo = new BioLinccDataLayer();
            source = _source;
            harvest_type_id = _harvest_type_id;
            harvest_id = _harvest_id;
        }

        public void GetInitialIDData()
        {
            // Preliminary processing of data
            // Allows trials that equate to more than one NCT registry tro be identified
            // Allows groups of Biolinnc trials that equate to a single NCT registry to be identified

            _logger.Information("Obtaining initial ID data to identify NCT matches");
            IEnumerable<StudyFileRecord> file_list = _mon_repo.FetchStudyFileRecords(source.id);
            int n = 0; string filePath = "";
            foreach (StudyFileRecord rec in file_list)
            {
                n++;
                filePath = rec.local_path;
                if (File.Exists(filePath))
                {
                    string inputString = "";
                    using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
                    {
                        inputString += streamReader.ReadToEnd();
                    }

                    XmlSerializer serializer = new XmlSerializer(typeof(BioLincc_Record));
                    StringReader rdr = new StringReader(inputString);
                    BioLincc_Record studyRegEntry = (BioLincc_Record)serializer.Deserialize(rdr);

                    // processing here focuses on the listed secondary identifiers...
                    identity_processor.ProcessData(studyRegEntry, storage_repo, _mon_repo, source.db_conn);
                }

                if (n % 10 == 0) _logger.Information(n.ToString());
            }

            identity_processor.CreateMultNCTsTable(biolincc_repo);
            _logger.Information("Table listing biolincc studies with multiple NCT numbers created");
            identity_processor.CreateMultHBLIsTable(biolincc_repo);
            _logger.Information("Table listing NCT records with multiple biolilncc studies created");
        }


        public int? LoopThroughFiles()
        {
            // Construct a list of the files using the sf records to get 
            // a list of files and local paths...

            _logger.Information("Obtaining main study data from web page");
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

                    XmlSerializer serializer = new XmlSerializer(typeof(BioLincc_Record));
                    StringReader rdr = new StringReader(inputString);
                    BioLincc_Record studyRegEntry = (BioLincc_Record)serializer.Deserialize(rdr);

                    // break up the file into relevant data classes
                    Study s = processor.ProcessData(studyRegEntry, rec.last_downloaded, biolincc_repo);

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
