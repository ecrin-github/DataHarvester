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
		public async Task HarvestDataAsync (Source source, int harvest_type_id, DateTime? cutoff_date, bool org_update_only)
		{

			// Identify source type and location, destination folder

			StringHelpers.SendFeedback("source_id is " + source.id.ToString());
			StringHelpers.SendFeedback("type_id is " + harvest_type_id.ToString());
			if (cutoff_date == null)
            {
				StringHelpers.SendFeedback("cutoff_date is not provided");
			}
            else 
			{
				StringHelpers.SendFeedback("cutoff_date is " + cutoff_date.ToString());
			}
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
				HarvestEvent harvest = new HarvestEvent(harvest_id, source.id, harvest_type_id, cutoff_date);
				

				// Harvest the data from the local XML files

				if (source.uses_who_harvest)
				{
					WHOController c = new WHOController(harvest_id, source, repo, logging_repo, harvest_type_id, cutoff_date);
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
								CTGController c = new CTGController(harvest_id, source, repo, logging_repo, harvest_type_id, cutoff_date);
								harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
								harvest.num_records_harvested = c.LoopThroughFiles();
								break;
							}
						case 100123:
							{
								EUCTRController c = new EUCTRController(harvest_id, source, repo, logging_repo, harvest_type_id, cutoff_date);
								harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
								harvest.num_records_harvested = c.LoopThroughFiles();
								break;
							}
						case 100126:
							{
								ISRCTNController c = new ISRCTNController(harvest_id, source, repo, logging_repo, harvest_type_id, cutoff_date);
								harvest.num_records_available = logging_repo.FetchFullFileCount(source.id, "study");
								harvest.num_records_harvested = await c.LoopThroughFilesAsync();
								break;
							}
						case 100135:
							{
								PubmedController c = new PubmedController(harvest_id, source, repo, logging_repo, harvest_type_id, cutoff_date);
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

			if (source.has_study_tables)
			{
				ppb.UpdateStudyIdentifierOrgs();
				StringHelpers.SendFeedback("study identifier orgs updated");
				ppb.UpdateStudyContributorOrgs();
				StringHelpers.SendFeedback("study contributor orgs updated");
			}
			ppb.UpdateDataObjectOrgs();
			StringHelpers.SendFeedback("data object managing orgs updated");
			ppb.StoreUnMatchedNames();
			StringHelpers.SendFeedback("unmatched org names stored");

			// Update and standardise topic ids and names
			string source_type = source.has_study_tables ? "study" : "object";
			ppb.UpdateTopics(source_type);
			StringHelpers.SendFeedback("topic data updated");

			ppb.DropContextForeignTables();

			// Note the hashes can only be done after all the
			// data is complete, including the organisation 
			// codes and names derived above

			HashBuilder hb = new HashBuilder(repo.ConnString, source);
			if (source.has_study_tables)
			{
				hb.CreateStudyHashes();
				StringHelpers.SendFeedback("study hashes created");
				hb.CreateStudyCompositeHashes();
				StringHelpers.SendFeedback("study composite hashes created");
			}
			hb.CreateDataObjectHashes();
			StringHelpers.SendFeedback("data object hashes created");
			hb.CreateObjectCompositeHashes();
			StringHelpers.SendFeedback("data object composite hashes created");
		}
	}
}


