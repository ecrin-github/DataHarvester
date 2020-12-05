using DataHarvester.biolincc;
using DataHarvester.ctg;
using DataHarvester.euctr;
using DataHarvester.isrctn;
using DataHarvester.pubmed;
using DataHarvester.who;
using DataHarvester.yoda;
using System;
using System.Threading.Tasks;

namespace DataHarvester
{
    class Harvester
    {
        public async Task HarvestDataAsync (Source source, int harvest_type_id, bool org_update_only, LoggingDataLayer logging_repo)
        {

            // Identify source type and location, destination folder
            logging_repo.LogHeader("Setup");
            logging_repo.LogLine("Source_id is " + source.id.ToString());
            logging_repo.LogLine("Type_id is " + harvest_type_id.ToString());
            logging_repo.LogLine("Update org ids only is " + org_update_only);

            DataLayer repo = new DataLayer(source.database_name);

            // Create sd tables. 
            // (Some sources may be data objects only.)
            if (!org_update_only)
            {
                // Construct the sd tables.

                SchemaBuilder sdb = new SchemaBuilder(repo.ConnString, source);
                if (source.has_study_tables)
                {
                    sdb.DeleteSDStudyTables();
                    sdb.BuildNewSDStudyTables();
                }
                sdb.DeleteSDObjectTables();
                sdb.BuildNewSDObjectTables();

                // construct the harvest_event record
                int harvest_id = logging_repo.GetNextHarvestEventId();
                HarvestEvent harvest = new HarvestEvent(harvest_id, source.id, harvest_type_id);

                // Harvest the data from the local XML files

                if (source.uses_who_harvest)
                {
                    WHOController c = new WHOController(harvest_id, source, repo, logging_repo, harvest_type_id);
                    harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
                    harvest.num_records_harvested = c.LoopThroughFiles();
                }
                else
                {
                    switch (source.id)
                    {
                        case 101900:
                            {
                                BioLinccController c = new BioLinccController(harvest_id, source, repo, logging_repo);
                                c.GetInitialIDData();
                                harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 101901:
                            {
                                YodaController c = new YodaController(harvest_id, source, repo, logging_repo);
                                harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100120:
                            {
                                CTGController c = new CTGController(harvest_id, source, repo, logging_repo, harvest_type_id);
                                harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100123:
                            {
                                EUCTRController c = new EUCTRController(harvest_id, source, repo, logging_repo, harvest_type_id);
                                harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
                                harvest.num_records_harvested = c.LoopThroughFiles();
                                break;
                            }
                        case 100126:
                            {
                                ISRCTNController c = new ISRCTNController(harvest_id, source, repo, logging_repo, harvest_type_id);
                                harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
                                harvest.num_records_harvested = await c.LoopThroughFilesAsync();
                                break;
                            }
                        case 100135:
                            {
                                PubmedController c = new PubmedController(harvest_id, source, repo, logging_repo, harvest_type_id);
                                harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "object");
                                harvest.num_records_harvested = c.LoopThroughFiles();

                                // For pubmed necessary to do additional processing afterwards 
                                // to identify publishers and agregate study linkage data
                                c.DoPubMedPostProcessing();
                                break;
                            }
                    }
                }

                harvest.time_ended = DateTime.Now;
                logging_repo.StoreHarvestEvent(harvest);
            }

            // These functions have to be run even if the 
            // harvest is 'org_update_only', as well as when the 
            // sd tables are constructed in the normal way

            PostProcBuilder ppb = new PostProcBuilder(repo.ConnString, source, logging_repo);
            ppb.EstablishContextForeignTables(repo.Username, repo.Password);

            // Update and standardise organisation ids and names
            logging_repo.LogHeader("Update Orgs and Topics");
            if (source.has_study_tables)
            {
                ppb.UpdateStudyIdentifierOrgs();
                logging_repo.LogLine("Study identifier orgs updated");
                ppb.UpdateStudyContributorOrgs();
                logging_repo.LogLine("Study contributor orgs updated");
            }
            ppb.UpdateDataObjectOrgs();
            logging_repo.LogLine("Data object managing orgs updated");
            ppb.StoreUnMatchedNames();
            logging_repo.LogLine("Unmatched org names stored");

            // Update and standardise topic ids and names
            string source_type = source.has_study_tables ? "study" : "object";
            ppb.UpdateTopics(source_type);
            logging_repo.LogLine("Topic data updated");

            ppb.DropContextForeignTables();

            // Note the hashes can only be done after all the
            // data is complete, including the organisation 
            // codes and names derived above
            logging_repo.LogHeader("Create Record Hashes");
            HashBuilder hb = new HashBuilder(repo.ConnString, source, logging_repo);
            if (source.has_study_tables)
            {
                hb.CreateStudyHashes();
                logging_repo.LogLine("Study hashes created");
                hb.CreateStudyCompositeHashes();
                logging_repo.LogLine("Study composite hashes created");
            }
            logging_repo.LogHeader("Create Composite Hashes");
            hb.CreateDataObjectHashes();
            logging_repo.LogLine("Data object hashes created");
            hb.CreateObjectCompositeHashes();
            logging_repo.LogLine("Data object composite hashes created");
        }
    }
}


