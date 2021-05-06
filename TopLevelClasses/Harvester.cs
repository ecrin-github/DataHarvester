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
using System.Xml.Serialization;

namespace DataHarvester
{
    class Harvester : IHarvester
    {
        ILogger _logger;
        ILoggerHelper _logger_helper;
        IMonitorDataLayer _mon_repo;
        IStorageDataLayer _storage_repo;

        int _harvest_type_id;
        bool _org_update_only;

        public Harvester(ILogger logger, ILoggerHelper logger_helper,
                         IMonitorDataLayer mon_repo, IStorageDataLayer storage_repo,
                         int harvest_type_id, bool org_update_only)
        {
            _logger = logger;
            _logger_helper = logger_helper;
            _mon_repo = mon_repo;
            _storage_repo = storage_repo;
            _harvest_type_id = harvest_type_id;
            _org_update_only = org_update_only;
        }

        public int Run(Options opts)
        {
            try
            {
                _logger_helper.Logheader("STARTING HARVESTER");
                _logger_helper.LogCommandLineParameters(opts);
                foreach (int source_id in opts.source_ids)
                {
                    HarvestData(source_id);
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


        private void HarvestData(int source_id)
        {
            // Obtain source details, augment with connection string for this database
            // after the storage repository for this source has been created.

            Source source = _mon_repo.FetchSourceParameters(source_id);
            Credentials creds = _mon_repo.Credentials;
            source.db_conn = creds.GetConnectionString(source.database_name, _harvest_type_id);
            _logger.Information("For source: " + source.id + ": " + source.database_name);

            if (!_org_update_only)
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
                HarvestEvent harvest = new HarvestEvent(harvest_id, source.id, _harvest_type_id);
                _logger.Information("Harvest event " + harvest_id.ToString() + " began");

                // Harvest the data from the local XML files
                harvest.num_records_available = _mon_repo.FetchFullFileCount(source.id, source.source_type, _harvest_type_id);

                if (source.uses_who_harvest)
                {
                    WHOProcessor processor = new WHOProcessor(_storage_repo, _mon_repo, _logger);
                    WHOController c = new WHOController(_logger, _mon_repo, _storage_repo, source, 
                                     _harvest_type_id, harvest_id, processor);
                    harvest.num_records_harvested = c.LoopThroughFiles();
                }
                else
                {
                    switch (source.id)
                    {
                        case 101900:
                            {
                                BioLinccProcessor processor = new BioLinccProcessor(_storage_repo, _mon_repo, _logger);
                                BioLinccController c = new BioLinccController(_logger, _mon_repo, _storage_repo, source,
                                                     _harvest_type_id, harvest_id, processor);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 101901:
                            {
                                
                                YodaProcessor processor = new YodaProcessor(_storage_repo, _mon_repo, _logger);
                                YodaController c = new YodaController(_logger, _mon_repo, _storage_repo, source,
                                                      _harvest_type_id, harvest_id, processor);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100120:
                            { 
                                CTGProcessor processor = new CTGProcessor(_storage_repo, _mon_repo, _logger);
                                CTGController c = new CTGController(_logger, _mon_repo, _storage_repo, source, 
                                                     _harvest_type_id, harvest_id, processor);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100123:
                            {
                                EUCTRProcessor processor = new EUCTRProcessor(_storage_repo, _mon_repo, _logger);
                                EUCTRController c = new EUCTRController(_logger, _mon_repo, _storage_repo, source,
                                                    _harvest_type_id, harvest_id, processor);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100126:
                            {
                                ISRCTNProcessor processor = new ISRCTNProcessor(_storage_repo, _mon_repo, _logger);
                                ISRCTNController c = new ISRCTNController(_logger, _mon_repo, _storage_repo, source,
                                                        _harvest_type_id, harvest_id, processor);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100135:
                            {
                                PubmedProcessor processor = new PubmedProcessor(_storage_repo, _mon_repo, _logger);
                                PubmedController c = new PubmedController(_logger, _mon_repo, _storage_repo, source,
                                                    _harvest_type_id, harvest_id, processor);
                                harvest.num_records_harvested = c.LoopThroughFiles();

                                // For pubmed necessary to do additional processing afterwards 
                                // to identify publishers and agregate study linkage data
                                c.DoPubMedPostProcessing();
                                break;
                            }
                    }
                }

                harvest.time_ended = DateTime.Now;
                _mon_repo.StoreHarvestEvent(harvest);

                _logger.Information("Number of source XML files: " + harvest.num_records_available.ToString());
                _logger.Information("Number of files harvested: " + harvest.num_records_harvested.ToString());
                _logger.Information("Harvest event " + harvest_id.ToString() + " ended");
            }

            // These functions have to be run even if the harvest is 'org_update_only'.

            ContextDataManager.Source context_source = new ContextDataManager.Source(source.id, source.preference_rating, source.database_name, source.db_conn,
                                                              source.has_study_tables, source.has_study_topics, source.has_study_contributors);
            ContextDataManager.Credentials context_creds = new ContextDataManager.Credentials(creds.Host, creds.Username, creds.Password);

            _logger_helper.Logheader("Updating context data");
            ContextMain context_entry_point = new ContextMain(_logger);
            context_entry_point.UpdateDataFromContext(context_creds, context_source);

            // Note the hashes can only be done after all the
            // data is complete, including the organisation 
            // codes and names derived above

            _logger_helper.Logheader("Create Record Hashes");
            HashBuilder hb = new HashBuilder(_logger, source, _mon_repo);
            if (source.has_study_tables)
            {
                hb.CreateStudyHashes();
                _logger.Information("Study hashes created");
                _logger.Information("Create Study Composite Hashes\n");
                hb.CreateStudyCompositeHashes();
                _logger.Information("Study composite hashes created");
            }
            hb.CreateDataObjectHashes();
            _logger.Information("Data object hashes created");
            _logger.Information("Create Object Composite Hashes\n");
            hb.CreateObjectCompositeHashes();
            _logger.Information("Data object composite hashes created");

            _logger_helper.LogTableStatistics(source, "sd");
        }

    }
}


