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

			Console.WriteLine("source_id is " + source.id.ToString());
			Console.WriteLine("type_id is " + harvest_type_id.ToString());
			if (cutoff_date == null)
            {
				Console.WriteLine("cutoff_date is not provided");
			}
            else 
			{
				Console.WriteLine("cutoff_date is " + cutoff_date.ToString());
			}
			Console.WriteLine("Update org ids only is " + org_update_only);

			LoggingDataLayer logging_repo = new LoggingDataLayer();
			DataLayer repo = new DataLayer(source.database_name);

			// Create sd tables. 
			// (Some sources may be data objects only.)
			if (!org_update_only)
			{
				// Construct the sd tables.

				SDBuilder sdb = new SDBuilder(repo.ConnString, source);
				if (source.has_study_tables)
				{
					sdb.DeleteSDStudyTables();
					sdb.BuildNewSDStudyTables();
				}
				sdb.DeleteSDObjectTables();
				sdb.BuildNewSDObjectTables();

				// Harvest the data from the local XML files

				if (source.uses_who_harvest)
				{
					WHOController c = new WHOController(source, repo, logging_repo, harvest_type_id, cutoff_date);
					c.LoopThroughFiles();
				}
				else
				{
					switch (source.id)
					{
						case 101900:
							{
								BioLinccController c = new BioLinccController(source, repo, logging_repo);
								c.GetInitialIDData();
								c.LoopThroughFiles();
								break;
							}
						case 101901:
							{
								YodaController c = new YodaController(source, repo, logging_repo);
								c.LoopThroughFiles();
								break;
							}
						case 100120:
							{
								CTGController c = new CTGController(source, repo, logging_repo, harvest_type_id, cutoff_date);
								c.LoopThroughFiles();
								break;
							}
						case 100123:
							{
								EUCTRController c = new EUCTRController(source, repo, logging_repo, harvest_type_id, cutoff_date);
								c.LoopThroughFiles();
								break;
							}
						case 100126:
							{
								ISRCTNController c = new ISRCTNController(source, repo, logging_repo, harvest_type_id, cutoff_date);
								await c.LoopThroughFilesAsync();
								break;
							}
						case 100135:
							{
								PubmedController c = new PubmedController(source, repo, logging_repo, harvest_type_id, cutoff_date);
								c.LoopThroughFiles();
								break; ;
							}
					}
				}
			}

			// These functions have to be run even if the 
			// harvest is 'org_update_only', as well as when the 
			// sd tables are constructged in the normal way

			HashBuilder hb = new HashBuilder(repo.ConnString, source);
			hb.EstablishContextForeignTables(repo.Username, repo.Password);
			hb.UpdateStudyIdentifierOrgs();
			hb.UpdateStudyContributorOrgs();
			hb.UpdateDataObjectOrgs();
			hb.DropContextForeignTables();

			hb.CreateStudyHashes();
			hb.CreateStudyCompositeHashes();
			hb.CreateDataObjectHashes();
			hb.CreateObjectCompositeHashes();
		}
	}
}


