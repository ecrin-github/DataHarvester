using System;
using static System.Console;
using DataHarvester.BioLincc;
using DataHarvester.Yoda;

namespace DataHarvester
{
	class Program
	{
		static void Main(string[] args)
		{
			int harvest_type_id = 0;
			int source_id = 0;
			if (args.Length == 2)
			{
				string source = args[0];
				switch (source.ToLower()[0])
				{
					case 'b':
						{
							source_id = 100900; break; // biolincc
						}
					case 'y':
						{
							source_id = 100901; break; // yoda
						}
				}

				if (source_id == 0)
				{
					WriteLine("sorry - I don't recognise that source argument");
				}


				string harvest_type = args[1];
				// should be 'a' or 't'
				// to indicate put all files into the sd tables ('a')
				// or just those files later than the last processing ('t')
				switch (harvest_type.ToLower()[0])
				{
					case 'a':
					{
						harvest_type_id = 1; break; // all files
					}
					case 't':
					{
						harvest_type_id = 2; break; // time specified files
					}
				}

				if (harvest_type_id == 0)
				{
					WriteLine("The second parameter needs to be 'a' or 't', or a word beginning with those letters");
				}
			}
			else
			{
				WriteLine("Wrong number of command line arguments - two are required");
				WriteLine("The first a string to indicate the source");
				WriteLine("The second a string to indicate the file harvest type");
			}

			// proceed if both required parameters are valid
			if (harvest_type_id > 0 && source_id > 0)
			{
				DataLayer repo = new DataLayer();

				switch (source_id)
				{
					case 100900:
						{
							BioLinccController biolincc_controller = new BioLinccController(repo, harvest_type_id, source_id);
							biolincc_controller.EstablishNewSDTables();
							biolincc_controller.LoopThroughFiles();
							biolincc_controller.UpdateIds();
							biolincc_controller.InsertHashes();
							break;
						}
					case 100901:
						{
							YodaController yoda_controller = new YodaController(repo, harvest_type_id, source_id);
							yoda_controller.LoopThroughFiles();
							break;
						}
				}
			}
		}
	}
}
