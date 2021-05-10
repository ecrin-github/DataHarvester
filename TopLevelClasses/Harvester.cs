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

        public Harvester(ILogger logger, ILoggerHelper logger_helper,
                         IMonitorDataLayer mon_repo, IStorageDataLayer storage_repo)
        {
            _logger = logger;
            _logger_helper = logger_helper;
            _mon_repo = mon_repo;
            _storage_repo = storage_repo;
        }

        public int Run(Options opts)
        {
            try
            {
                _logger_helper.Logheader("STARTING HARVESTER");
                _logger_helper.LogCommandLineParameters(opts);
                int harvest_type_id = opts.harvest_type_id;
                bool org_update_only = opts.org_update_only;

                foreach (int source_id in opts.source_ids)
                {
                    HarvestData(source_id, harvest_type_id, org_update_only);
                }

                _logger_helper.Logheader("Closing Log");
                return 0;
            }

            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger_helper.Logheader("Closing Log");
                return -1;
            }
        }


        private void HarvestData(int source_id, int harvest_type_id, bool org_update_only)
        {
            // Obtain source details, augment with connection string for this database

            ISource source = _mon_repo.FetchSourceParameters(source_id);
            Credentials creds = _mon_repo.Credentials;
            source.db_conn = creds.GetConnectionString(source.database_name, harvest_type_id);
            _logger.Information("For source: " + source.id + ": " + source.database_name);

            if (!org_update_only)
            {
                // Bulk of the harvesting process can be skipped if this run is just for updating 
                // tables with context values. Otherwise...
                // construct the sd tables. (Some sources may be data objects only.)

                _logger_helper.Logheader("Recreate database tables");
                SchemaBuilder sdb = new SchemaBuilder(source, _logger);
                sdb.RecreateTables();

                // Construct the harvest_event record.

                _logger_helper.Logheader("Process data");
                int harvest_id = _mon_repo.GetNextHarvestEventId();
                HarvestEvent harvest = new HarvestEvent(harvest_id, source.id, harvest_type_id);
                _logger.Information("Harvest event " + harvest_id.ToString() + " began");

                // Harvest the data from the local XML files
                IStudyProcessor study_processor = null;
                IObjectProcessor object_processor = null;
                harvest.num_records_available = _mon_repo.FetchFullFileCount(source.id, source.source_type, harvest_type_id);

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
                    harvest.num_records_harvested = c.LoopThroughFiles(harvest_type_id, harvest_id);
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
                    harvest.num_records_harvested = c.LoopThroughFiles(harvest_type_id, harvest_id);
                    c.DoPostProcessing();
                }  

                harvest.time_ended = DateTime.Now;
                _mon_repo.StoreHarvestEvent(harvest);

                _logger.Information("Number of source XML files: " + harvest.num_records_available.ToString());
                _logger.Information("Number of files harvested: " + harvest.num_records_harvested.ToString());
                _logger.Information("Harvest event " + harvest_id.ToString() + " ended");
            }

            // These functions have to be run even if the harvest is 'org_update_only'.

            ContextDataManager.Source context_source = new ContextDataManager.Source(source.id, source.database_name, source.db_conn,
                                                       source.has_study_tables, source.has_study_topics, source.has_study_contributors);
            ContextDataManager.Credentials context_creds = new ContextDataManager.Credentials(creds.Host, creds.Username, creds.Password);

            _logger_helper.Logheader("Updating context data");
            ContextMain context_entry_point = new ContextMain(_logger);
            context_entry_point.UpdateDataFromContext(context_creds, context_source);

            // Note the hashes can only be done after all the data is complete, including 
            // the organisation codes and names derived above

            HashDataLibrary.Source hash_source = new HashDataLibrary.Source(source.id, source.database_name, source.db_conn,
                      source.has_study_tables, source.has_study_topics, source.has_study_features,
                      source.has_study_contributors, source.has_study_references, source.has_study_relationships,
                      source.has_study_links, source.has_study_ipd_available, source.has_object_datasets,
                      source.has_object_dates, source.has_object_rights, source.has_object_relationships,
                      source.has_object_pubmed_set);

            _logger_helper.Logheader("Creating Record Hashes");
            HashMain hash_entry_point = new HashMain(_logger);
            hash_entry_point.HashData(hash_source);

            // finally summarise results by p[roviding stats on the sd tables
            _logger_helper.LogTableStatistics(source, "sd");
        }
    }
}


