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
	class Program
	{
		static async Task Main(string[] args)
		{
			// Identify source folder, destination database, harvest type and 
			// any cutoff date from the arguments provided.

			if (NoArgsProvided(args)) return;
			int source_id = GetFirstArg(args[0]);
			if (source_id == 0) return;

			LoggingDataLayer logging_repo = new LoggingDataLayer();
			Source source = logging_repo.FetchSourceParameters(source_id);
			if (source == null)
			{
				WriteLine("Sorry - the first argument does not correspond to a known source");
				return;
			}

			int harvest_type_id = GetHarvestType(args, source);
			DateTime? harvest_cutoff_revision_date = harvest_type_id == 2 ? GetHarvestCutOffDate(args) : null;
			if (harvest_type_id == 2 && harvest_cutoff_revision_date == null)
			{
				WriteLine("Sorry - there must be a valid cutoff date for a harvest of type 2");
				return;
			}

			DataLayer repo = new DataLayer(source.database_name);

			// Create sd tables. 
			// (Some sources may be data objects only.)

			SDBuilder sdb = new SDBuilder(repo.ConnString, source);
			if (source.has_study_tables)
			{
				sdb.DeleteSDStudyTables();
				sdb.BuildNewSDStudyTables();
			}
			sdb.DeleteSDObjectTables();
			sdb.BuildNewSDObjectTables();

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
						CTGController c = new CTGController(source, repo, logging_repo);
						await c.LoopThroughFilesAsync();
						break;
					}
				case 100123:
					{
						EUCTRController c = new EUCTRController(source, repo, logging_repo);
						await c.LoopThroughFilesAsync();
						break;
					}
				case 100126:
					{
						ISRCTNController c = new ISRCTNController(source, repo, logging_repo);
						await c.LoopThroughFilesAsync();
						break;
					}
				case 100115:
					{
						WHOController c = new WHOController(source, repo, logging_repo);
						await c.LoopThroughFilesAsync();
						break;
					}
				case 100135:
					{
						PubmedController c = new PubmedController(source, repo, logging_repo);
						await c.LoopThroughFilesAsync();
						break;;
					}
			}

			HashBuilder hb = new HashBuilder(repo.ConnString, source);
			hb.EstablishContextForeignTables(repo.Username, repo.Password);
			hb.UpdateStudyIdentifierOrgs();
			hb.UpdateDataObjectOrgs();
			hb.DropContextForeignTables();

			hb.CreateStudyHashes();
			hb.CreateStudyCompositeHashes();
			hb.CreateDataObjectHashes();
			hb.CreateObjectCompositeHashes();
		}



		private static bool NoArgsProvided(string[] args)
		{
			if (args.Length == 0)
			{
				WriteLine("Sorry - one, two or three parameters are necessary");
				WriteLine("The first is a 6 digit number to indicate the source.");
				WriteLine("The second (optional) either 1, 2 or 3 to indicate if all (1) ");
				WriteLine("or a time limited set of files (2) or ");
				WriteLine("the files marked as non-complete (3) are to be imported");
				WriteLine("if the second parameter is 2 a third parameter is required ");
				WriteLine("which should be a date in YYYY-MM-DD format - only files with");
				WriteLine("a revision date later than this date will be harvested.");
				WriteLine("Any additional parameters will be ignored.");
				return true;
			}
			else
			{
				return false;
			}
		}


		private static int GetFirstArg(string arg)
		{
			int arg_id = 0;
			if (!Int32.TryParse(arg, out arg_id))
			{
				WriteLine("Sorry - the first argument must be an integer");
			}
			return arg_id;
		}


		private static int GetHarvestType(string[] args, Source source_parameters)
		{
			if (args.Length > 1)
			{
				int harvest_type_arg = 0;
				if (!Int32.TryParse(args[1], out harvest_type_arg))
				{
					WriteLine("The second argument, if present, must be an integer (default settinmg will be used)");
					harvest_type_arg = source_parameters.default_harvest_type_id;
				}
				if (harvest_type_arg != 1 && harvest_type_arg != 2 && harvest_type_arg != 3)
				{
					WriteLine("Sorry - the second argument, if present, must be 1, 2, or 3 (default settinmg will be used)");
					harvest_type_arg = source_parameters.default_harvest_type_id;
				}
				return harvest_type_arg;
			}
			else
			{
				// use the default harvesting method
				return source_parameters.default_harvest_type_id;
			}
		}


		private static DateTime? GetHarvestCutOffDate(string[] args)
		{
				if (args.Length < 3)
				{
					WriteLine("Sorry - if the second argument is 2, ");
					WriteLine("(harvest only files revised after a set date)");
					WriteLine("You must include a third date parameter in the format YYYY-MM-DD");
					return null;
				}

				if (!Regex.Match(args[2], @"^20\d{2}-[0,1]\d{1}-[0, 1, 2, 3]\d{1}$").Success)
				{
					WriteLine("Sorry - if the second argument is 2, "); ;
					WriteLine("(harvest only files revised after a set date)");
					WriteLine("The third parameter must be in in the format YYYY-MM-DD");
					return null;
				}

				return new DateTime(Int32.Parse(args[2].Substring(0, 4)),
									Int32.Parse(args[2].Substring(5, 2)), 
									Int32.Parse(args[2].Substring(8, 2)));
		}
	}
}


