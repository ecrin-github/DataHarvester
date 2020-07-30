using System;
using static System.Console;
using System.Text.RegularExpressions;
using CommandLine;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Linq;

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
			if (harvest_type_id != 1 && harvest_type_id != 2 && harvest_type_id != 3)
			{
				WriteLine("Sorry - the harvest type argument does not correspond to 1, 2 or 3");
				return -1;
			}


			// If a date is required check one is present and is valid. 
			// It should be in the ISO YYYY-MM-DD format.
			DateTime? cutoff_date = null;
			if (harvest_type_id == 2)
			{
				string cutoff_string = opts.cutoff_date;
				if (!string.IsNullOrEmpty(cutoff_string))
				{
					if (Regex.Match(cutoff_string, @"^20\d{2}-[0,1]\d{1}-[0, 1, 2, 3]\d{1}$").Success)
					{
						cutoff_date = new DateTime(
									Int32.Parse(cutoff_string.Substring(0, 4)),
									Int32.Parse(cutoff_string.Substring(5, 2)),
									Int32.Parse(cutoff_string.Substring(8, 2)));
					}

				}

				if (cutoff_date == null)
				{
					WriteLine("Sorry - this harvest type requires a date"); ;
					WriteLine("in the format YYYY-MM-DD and this is missing");
					return -1;
				}
			}

			Harvester dl = new Harvester();

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
						await dl.HarvestDataAsync(source, harvest_type_id, cutoff_date, opts.org_update_only);
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

		[Option('t', "harvest_type_id", Required = true, HelpText = "Integer representing type of harvest (1 = full, 2 = with cutoff date, 3 = incomplete files only).")]
		public int harvest_type_id { get; set; }

		[Option('d', "cutoff_date", Required = false, HelpText = "Only data revised or added since this date will be considered")]
		public string cutoff_date { get; set; }

		[Option('G', "organisation_update_only", Required = false, HelpText = "If present does not recreate Sd tables - only updates organisation ids")]
		public bool org_update_only { get; set; }

	}
}




