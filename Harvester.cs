using System;
using static System.Console;
using System.Text.RegularExpressions;
using DataHarvester.biolincc;
using System.Threading.Tasks;
using DataHarvester.yoda;
using DataHarvester.euctr;
using DataHarvester.isrctn;
using DataHarvester.ctg;
using DataHarvester.pubmed;
using DataHarvester.who;

namespace DataHarvester
{
	class Harvester
	{
		public async Task HarvestDataAsync (Source source, int harvest_type_id, bool org_update_only)
		{

			// Identify source type and location, destination folder
			StringHelpers.SendHeader("Setup");
			StringHelpers.SendFeedback("Source_id is " + source.id.ToString());
			StringHelpers.SendFeedback("Type_id is " + harvest_type_id.ToString());
			StringHelpers.SendFeedback("Update org ids only is " + org_update_only);

			LoggingDataLayer logging_repo = new LoggingDataLayer();
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

			PostProcBuilder ppb = new PostProcBuilder(repo.ConnString, source);
			ppb.EstablishContextForeignTables(repo.Username, repo.Password);

			// Update and standardise organbisation ids and names
			StringHelpers.SendHeader("Update Orgs and Topics");
			if (source.has_study_tables)
			{
				ppb.UpdateStudyIdentifierOrgs();
				StringHelpers.SendFeedback("Study identifier orgs updated");
				ppb.UpdateStudyContributorOrgs();
				StringHelpers.SendFeedback("Study contributor orgs updated");
			}
			ppb.UpdateDataObjectOrgs();
			StringHelpers.SendFeedback("Data object managing orgs updated");
			ppb.StoreUnMatchedNames();
			StringHelpers.SendFeedback("Unmatched org names stored");

			// Update and standardise topic ids and names
			string source_type = source.has_study_tables ? "study" : "object";
			ppb.UpdateTopics(source_type);
			StringHelpers.SendFeedback("Topic data updated");

			ppb.DropContextForeignTables();

			// Note the hashes can only be done after all the
			// data is complete, including the organisation 
			// codes and names derived above
			StringHelpers.SendHeader("Create Record Hashes");
			HashBuilder hb = new HashBuilder(repo.ConnString, source);
			if (source.has_study_tables)
			{
				hb.CreateStudyHashes();
				StringHelpers.SendFeedback("Study hashes created");
				hb.CreateStudyCompositeHashes();
				StringHelpers.SendFeedback("Study composite hashes created");
			}
			StringHelpers.SendHeader("Create Composite Hashes");
			hb.CreateDataObjectHashes();
			StringHelpers.SendFeedback("Data object hashes created");
			hb.CreateObjectCompositeHashes();
			StringHelpers.SendFeedback("Data object composite hashes created");
		}
	}
}


