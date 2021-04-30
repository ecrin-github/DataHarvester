using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Serilog;


namespace DataHarvester.isrctn
{
    class ISRCTNController
    {
        ILogger _logger;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer _storage_repo;
        XmlSerializer _serializer;
        ISRCTNProcessor _processor;
        Source source;
        int harvest_type_id;
        int harvest_id; 

        public ISRCTNController(ILogger logger, IMonitorDataLayer mon_repo, IStorageDataLayer storage_repo,
                                Source _source, int _harvest_type_id, int _harvest_id, 
                                XmlSerializer serializer, ISRCTNProcessor processor)
        {
            _logger = logger;
            _mon_repo = mon_repo;
            _storage_repo = storage_repo;
            _serializer = serializer;
            _processor = processor;
            source = _source;
            harvest_type_id = _harvest_type_id;
            harvest_id = _harvest_id;
        }

        public int? LoopThroughFiles()
        {
            // Get the folder base from the appsettings file
            // and construct a list of the files 
            // N.B. (only one folder for all files) 

            // Construct a list of the files 
            // Rather than using a file base, it is possible
            // to use the sf records to get a list of files
            // and local paths...

            IEnumerable<StudyFileRecord> file_list = _mon_repo.FetchStudyFileRecords(source.id, harvest_type_id);
            int n = 0; string filePath = "";
            foreach (StudyFileRecord rec in file_list)
            {
                n++;
                // for testing...
                // if (n == 100) break;

                filePath = rec.local_path;
                if (File.Exists(filePath))
                {
                    string inputString = "";
                    using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
                    {
                        inputString += streamReader.ReadToEnd();
                    }

                    StringReader rdr = new StringReader(inputString);
                    var studyEntry = _serializer.Deserialize(rdr);

                    // break up the file into relevant data classes
                    Study s = _processor.ProcessData(studyEntry, rec.last_downloaded);

                    // store the data in the database
                    _processor.StoreData(s, source.db_conn);

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
