using DataHarvester.biolincc;
using DataHarvester.ctg;
using DataHarvester.euctr;
using DataHarvester.isrctn;
using DataHarvester.pubmed;
using DataHarvester.who;
using DataHarvester.yoda;
using Serilog;
using System;
using System.Linq;
using ContextDataManager;
using HashDataLibrary;
using System.Xml.Serialization;

namespace DataHarvester
{
    class Harvester : IHarvester
    {
        ILogger _logger;
        ILoggerHelper _logger_helper;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer _storage_repo;
        ITestingDataLayer _test_repo;

        public Harvester(ILogger logger, ILoggerHelper logger_helper,
                         IMonitorDataLayer mon_repo, IStorageDataLayer storage_repo, 
                         ITestingDataLayer test_repo
                         )
        {
            _logger = logger;
            _logger_helper = logger_helper;
            _mon_repo = mon_repo;
            _storage_repo = storage_repo;
            _test_repo = test_repo;
        }

        public int Run(Options opts)
        {
            try
            {
                _logger_helper.LogHeader("STARTING HARVESTER");
                _logger_helper.LogCommandLineParameters(opts);
                foreach (int source_id in opts.source_ids)
                {
                    HarvestData(source_id, opts);
                }
                _logger_helper.LogHeader("Closing Log");
                return 0;
            }

            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger_helper.LogHeader("Closing Log");
                return -1;
            }
        }

        private void HarvestData(int source_id, Options opts)
        {
            // Obtain source details, augment with connection string for this database

            ISource source = _mon_repo.FetchSourceParameters(source_id);
            Credentials creds = _mon_repo.Credentials;
            source.db_conn = creds.GetConnectionString(source.database_name, opts.harvest_type_id);
            _logger_helper.LogStudyHeader(opts, "For source: " + source.id + ": " + source.database_name);

            if (!opts.org_update_only)
            {
                // Bulk of the harvesting process can be skipped if this run is just for updating 
                // tables with context values. 

                if (source.source_type == "test")
                {
                    // Set up expected data for later processing.
                    // This is data derived from manual inspection of files and requires
                    // a very different method, using stored procedures in the test db
                    _test_repo.EstablishExpectedData();   
                }
                else
                {   
                    // Otherwise...
                    // construct the sd tables. (Some sources may be data objects only.)

                    _logger_helper.LogHeader("Recreate database tables");
                    SchemaBuilder sdb = new SchemaBuilder(source, _logger);
                    sdb.RecreateTables();

                    // Construct the harvest_event record.

                    _logger_helper.LogHeader("Process data");
                    int harvest_id = _mon_repo.GetNextHarvestEventId();
                    HarvestEvent harvest = new HarvestEvent(harvest_id, source.id, opts.harvest_type_id);
                    _logger.Information("Harvest event " + harvest_id.ToString() + " began");

                    // Harvest the data from the local XML files
                    IStudyProcessor study_processor = null;
                    IObjectProcessor object_processor = null;
                    harvest.num_records_available = _mon_repo.FetchFullFileCount(source.id, source.source_type, opts.harvest_type_id);

                    if (source.source_type == "study")
                    {
                        if (source.uses_who_harvest)
                        {
                            study_processor = new WHOProcessor(_mon_repo, _logger);
                        }
                        else
                        {
                            switch (source.id)
                            {
                                case 101900:
                                    {
                                        study_processor = new BioLinccProcessor(_mon_repo, _logger);
                                        break;
                                    }
                                case 101901:
                                    {
                                        study_processor = new YodaProcessor(_mon_repo, _logger);
                                        break;
                                    }
                                case 100120:
                                    {
                                        study_processor = new CTGProcessor(_mon_repo, _logger);
                                        break;
                                    }
                                case 100123:
                                    {
                                        study_processor = new EUCTRProcessor(_mon_repo, _logger);
                                        break;
                                    }
                                case 100126:
                                    {
                                        study_processor = new ISRCTNProcessor(_mon_repo, _logger);
                                        break;
                                    }
                            }
                        }

                        StudyController c = new StudyController(_logger, _mon_repo, _storage_repo, source, study_processor);
                        harvest.num_records_harvested = c.LoopThroughFiles(opts.harvest_type_id, harvest_id);
                    }
                    else
                    {
                        // source type is 'object'
                        switch (source.id)
                        {
                            case 100135:
                                {
                                    object_processor = new PubmedProcessor(_mon_repo, _logger);
                                    break;
                                }
                        }

                        ObjectController c = new ObjectController(_logger, _mon_repo, _storage_repo, source, object_processor);
                        harvest.num_records_harvested = c.LoopThroughFiles(opts.harvest_type_id, harvest_id);
                    }

                    harvest.time_ended = DateTime.Now;
                    _mon_repo.StoreHarvestEvent(harvest);

                    _logger.Information("Number of source XML files: " + harvest.num_records_available.ToString());
                    _logger.Information("Number of files harvested: " + harvest.num_records_harvested.ToString());
                    _logger.Information("Harvest event " + harvest_id.ToString() + " ended");
                }
            }

            // The functions below have to be run even if the harvest is 'org_update_only',
            // and also for the expected data in the test database.

            // -------------------------------------------------------------------
            // MAKE USE OF SEPARATE 'CONTEXT' PROJECT (Same Solution, not DLL) 
            // -------------------------------------------------------------------

            ContextDataManager.Source context_source = new ContextDataManager.Source(source.id, source.source_type, source.database_name, source.db_conn,
                                                       source.has_study_tables, source.has_study_topics, source.has_study_contributors);
            ContextDataManager.Credentials context_creds = new ContextDataManager.Credentials(creds.Host, creds.Username, creds.Password);

            _logger_helper.LogHeader("Updating context data");
            ContextMain context_main = new ContextMain(_logger);

            string schema = (source.source_type == "test") ? "expected" : "sd";
            context_main.UpdateDataFromContext(context_creds, context_source, schema);

            // -------------------------------------------------------------------
            // MAKE USE OF SEPARATE 'HASH' PROJECT (Same Solution, not DLL) 
            // -------------------------------------------------------------------

            // Note the hashes can only be done after all the data is complete, including 
            // the organisation and topic codes and names derived above

            HashDataLibrary.Source hash_source = new HashDataLibrary.Source(source.id, source.database_name, source.db_conn,
                      source.has_study_tables, source.has_study_topics, source.has_study_features,
                      source.has_study_contributors, source.has_study_references, source.has_study_relationships,
                      source.has_study_links, source.has_study_ipd_available, source.has_object_datasets,
                      source.has_object_dates, source.has_object_rights, source.has_object_relationships,
                      source.has_object_pubmed_set);

            _logger_helper.LogHeader("Creating Record Hashes");
            HashMain hash_main = new HashMain(_logger);
            hash_main.HashData(hash_source, schema);

            // If harvesting test data it needs to be transferred  
            // to the sdcomp schema for safekeeping and further processing
            // If a normal harvest from a full source statistics should be produced.
            // If the harvest was of the manual 'expected' data do neither.

            if (source.source_type != "test")
            {
                if (opts.harvest_type_id == 3)
                {
                    // transfer sd data to test composite data store for later comparison
                    // otherwise it will be overwritten by the next harvest of sd data
                    _test_repo.TransferTestSDData(source);
                }
                else
                {
                    // summarise results by providing stats on the sd tables
                    _logger_helper.LogTableStatistics(source, "sd");
                }
            }
        }
    }
}


