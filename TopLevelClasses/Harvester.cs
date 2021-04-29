using DataHarvester.biolincc;
using DataHarvester.ctg;
using DataHarvester.euctr;
using DataHarvester.isrctn;
using DataHarvester.pubmed;
using DataHarvester.who;
using DataHarvester.yoda;
using System;
using System.Threading.Tasks;
using Serilog;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Linq;

namespace DataHarvester
{
    class Harvester : IHarvester
    {
        ILogger _logger;
        IConfiguration _settings;
        IMonitorDataLayer _mon_repo;

        public Harvester(ILogger logger, IConfiguration configFiles, IMonitorDataLayer mon_repo)
        {
            _logger = logger;
            _settings = configFiles;
            _mon_repo = mon_repo;
        }

        public async Task<int> RunAsync(Options opts)
        {
            try
            {
                _logger.Information("STARTING HARVESTER\n");
                LogCommandLineParameters(opts, _logger);

                foreach (int source_id in opts.source_ids)
                {
                    await HarvestDataAsync(source_id, opts.harvest_type_id, opts.org_update_only, _mon_repo);
                }
                _logger.Information("Closing Log\n");
                return 0;
            }

            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger.Information("Closing Log\n");
                return -1;
            }

        }


        private async Task HarvestDataAsync(int source_id, int harvest_type_id, bool org_update_only, IMonitorDataLayer mon_repo)
        {
            Source source = mon_repo.FetchSourceParameters(source_id);
            StorageDataLayer storage_repo = new StorageDataLayer(source.database_name, mon_repo.Credentials, harvest_type_id);

            // Create sd tables. (Some sources may be data objects only.)

            if (org_update_only != true)
            {
                // Construct the sd tables.

                SchemaBuilder sdb = new SchemaBuilder(storage_repo.ConnString, source);
                if (source.has_study_tables)
                {
                    sdb.DeleteSDStudyTables();
                    sdb.BuildNewSDStudyTables();
                }
                sdb.DeleteSDObjectTables();
                sdb.BuildNewSDObjectTables();

                // construct the harvest_event record
                int harvest_id = mon_repo.GetNextHarvestEventId();
                HarvestEvent harvest = new HarvestEvent(harvest_id, source.id, harvest_type_id);

                // Harvest the data from the local XML files

                if (source.uses_who_harvest)
                {
                    WHOController c = new WHOController(_logger, mon_repo, storage_repo, source, harvest_type_id, harvest_id);
                    harvest.num_records_available = mon_repo.FetchFullFileCount(source.id, "study", harvest_type_id);
                    harvest.num_records_harvested = c.LoopThroughFiles();
                }
                else
                {
                    switch (source.id)
                    {
                        case 101900:
                            {
                                BioLinccController c = new BioLinccController(_logger, mon_repo, storage_repo, source, harvest_id);
                                c.GetInitialIDData();
                                harvest.num_records_available = mon_repo.FetchFullFileCount(source.id, "study", harvest_type_id);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 101901:
                            {
                                YodaController c = new YodaController(_logger, mon_repo, storage_repo, source, harvest_id);
                                harvest.num_records_available = mon_repo.FetchFullFileCount(source.id, "study", harvest_type_id);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100120:
                            {
                                CTGController c = new CTGController(_logger, mon_repo, storage_repo, source, harvest_id, harvest_type_id);
                                harvest.num_records_available = mon_repo.FetchFullFileCount(source.id, "study", harvest_type_id);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100123:
                            {
                                EUCTRController c = new EUCTRController(_logger, mon_repo, storage_repo, source, harvest_id, harvest_type_id);
                                harvest.num_records_available = mon_repo.FetchFullFileCount(source.id, "study", harvest_type_id);
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100126:
                            {
                                ISRCTNController c = new ISRCTNController(_logger, mon_repo, storage_repo, source, harvest_id, harvest_type_id);
                                harvest.num_records_available = mon_repo.FetchFullFileCount(source.id, "study", harvest_type_id);
                                harvest.num_records_harvested = await c.LoopThroughFilesAsync();
                                break;
                            }
                        case 100135:
                            {
                                PubmedController c = new PubmedController(_logger, mon_repo, storage_repo, source, harvest_id, harvest_type_id);
                                harvest.num_records_available = mon_repo.FetchFullFileCount(source.id, "object", harvest_type_id);
                                harvest.num_records_harvested = c.LoopThroughFiles();

                                // For pubmed necessary to do additional processing afterwards 
                                // to identify publishers and agregate study linkage data
                                c.DoPubMedPostProcessing();
                                break;
                            }
                    }
                }

                harvest.time_ended = DateTime.Now;
                mon_repo.StoreHarvestEvent(harvest);
            }

            // These functions have to be run even if the 
            // harvest is 'org_update_only', as well as when the 
            // sd tables are constructed in the normal way

            PostProcBuilder ppb = new PostProcBuilder(storage_repo.ConnString, source, mon_repo, _logger);
            ppb.EstablishContextForeignTables(storage_repo.Credentials.Username, storage_repo.Credentials.Password);

            // Update and standardise organisation ids and names
            _logger.Information("Update Orgs and Topics\n");
            if (source.has_study_tables)
            {
                ppb.UpdateStudyIdentifierOrgs();
                _logger.Information("Study identifier orgs updated");
                ppb.UpdateStudyContributorOrgs();
                _logger.Information("Study contributor orgs updated");
            }
            ppb.UpdateDataObjectOrgs();
            _logger.Information("Data object managing orgs updated");
            ppb.StoreUnMatchedNames();
            _logger.Information("Unmatched org names stored");

            // Update and standardise topic ids and names
            string source_type = source.has_study_tables ? "study" : "object";
            ppb.UpdateTopics(source_type);
            _logger.Information("Topic data updated");

            ppb.DropContextForeignTables();

            // Note the hashes can only be done after all the
            // data is complete, including the organisation 
            // codes and names derived above
            _logger.Information("Create Record Hashes\n");
            HashBuilder hb = new HashBuilder(_logger, storage_repo.ConnString, source, mon_repo);
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

            LoggerHelper log_helper = new LoggerHelper(_logger);
            log_helper.LogTableStatistics(mon_repo.Credentials, source);
        }


        private void LogCommandLineParameters(Options opts, ILogger logger)
        {
            logger.Information("Setup\n");
            int[] source_ids = opts.source_ids.ToArray();
            if (source_ids.Length == 1)
            {
                logger.Information("Source_id is " + source_ids[0].ToString());
            }
            else
            {
                logger.Information("Source_ids are " + string.Join(",", source_ids));
            }
            logger.Information("Type_id is " + opts.harvest_type_id.ToString());
            logger.Information("Update org ids only is " + opts.org_update_only);
        }
    }
}


