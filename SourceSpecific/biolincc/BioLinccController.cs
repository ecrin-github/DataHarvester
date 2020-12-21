using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;

namespace DataHarvester.biolincc
{
    public class BioLinccController
    {
        DataLayer common_repo;
        LoggingDataLayer logging_repo;
        BioLinccDataLayer biolincc_repo;
        BioLinccProcessor processor;
        BioLinccIdentifierProcessor identity_processor;
        Source source;
        int last_harvest_id;

        public BioLinccController(int _last_harvest_id, Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo)
        {
            source = _source;
            processor = new BioLinccProcessor();
            identity_processor = new BioLinccIdentifierProcessor();
            common_repo = _common_repo;
            logging_repo = _logging_repo;
            biolincc_repo = new BioLinccDataLayer();
            last_harvest_id = _last_harvest_id;
        }

        public void GetInitialIDData()
        {
            // Preliminary processing of data
            // Allows trials that equate to more than one NCT registry tro be identified
            // Allows groups of Biolinnc trials that equate to a single NCT registry to be identified

            IEnumerable<StudyFileRecord> file_list = logging_repo.FetchStudyFileRecords(source.id);
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
                    identity_processor.ProcessData(studyRegEntry, common_repo, logging_repo);
                }

                if (n % 10 == 0) logging_repo.LogLine(n.ToString());
            }

            identity_processor.CreateMultNCTsTable(biolincc_repo);
            identity_processor.CreateMultHBLIsTable(biolincc_repo);
        }


        public int? LoopThroughFiles()
        {
            // Construct a list of the files using the sf records to get 
            // a list of files and local paths...

            IEnumerable<StudyFileRecord> file_list = logging_repo.FetchStudyFileRecords(source.id);
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
                    Study s = processor.ProcessData(studyRegEntry, rec.last_downloaded, common_repo, biolincc_repo, logging_repo);

                    // store the data in the database			
                    processor.StoreData(common_repo, s, logging_repo);

                    // update file record with last processed datetime
                    logging_repo.UpdateFileRecLastHarvested(rec.id, "study", last_harvest_id);

                }

                if (n % 10 == 0) logging_repo.LogLine(n.ToString());
            }

            return n;
        }


    }

}
