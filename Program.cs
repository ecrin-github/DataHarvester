using CommandLine;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static System.Console;

namespace DataHarvester
{
    class Program
	{

		static async Task Main(string[] args)
		{
			var parsedArguments = Parser.Default.ParseArguments<Options>(args);
			await parsedArguments.WithParsedAsync(opts => RunOptionsAndReturnExitCodeAsync(opts));
			await parsedArguments.WithNotParsedAsync((errs) => HandleParseErrorAsync(errs));
		}

		static async Task<int> RunOptionsAndReturnExitCodeAsync(Options opts)
		{
			// Check harvest type id is valid. 

			int harvest_type_id = opts.harvest_type_id;
			if (harvest_type_id != 1 && harvest_type_id != 2)
			{
				WriteLine("Sorry - the harvest type argument does not correspond to 1 or 2");
				return -1;
			}

			Harvester dh = new Harvester();

			// Check each source id is valid and run the program if it is... 

			LoggingDataLayer logging_repo = new LoggingDataLayer();
			if (opts.source_ids.Count() > 0)
            {
				foreach (int source_id in opts.source_ids)
				{
					Source source = logging_repo.FetchSourceParameters(source_id);
					if (source == null)
					{
						WriteLine("Sorry - the first argument does not correspond to a known source");
						return -1;
					}
					else
					{
						logging_repo.OpenLogFile(source.database_name);
						await dh.HarvestDataAsync(source, harvest_type_id, opts.org_update_only, logging_repo);
					}
				}
			}
			
			return 0;
		}

		static Task HandleParseErrorAsync(IEnumerable<Error> errs)
		{
			// do nothing for the moment
			return Task.CompletedTask;
		}

	}


	public class Options
	{
		// Lists the command line arguments and options

		[Option('s', "source_ids", Required = true, Separator = ',', HelpText = "Comma separated list of Integer ids of data sources.")]
		public IEnumerable<int> source_ids { get; set; }

		[Option('t', "harvest_type_id", Required = true, HelpText = "Integer representing type of harvest (1 = full, i.e. all available files, 2 = only files downloaded since last import.")]
		public int harvest_type_id { get; set; }

		//[Option('d', "cutoff_date", Required = false, HelpText = "Only data revised or added since this date will be considered")]
		//public string cutoff_date { get; set; }

		[Option('G', "organisation_update_only", Required = false, HelpText = "If present does not recreate Sd tables - only updates organisation ids")]
		public bool org_update_only { get; set; }

	}
}




