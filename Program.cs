using System;
using static System.Console;
using DataHarvester.BioLincc;
using DataHarvester.Yoda;
using DataHarvester.DBHelpers;

namespace DataHarvester
{
	class Program
	{
		static void Main(string[] args)
		{

			string source;
			int harvest_type_id = 0;

			if (args.Length == 0 || args.Length > 2)
			{
				WriteLine("sorry - one or two parameters are necessary");
				WriteLine("The first a letter to indicate the source");
				WriteLine("The second (optional) either a or t to indicate if all or time limited set of files are to be imported");
			}
			else
			{
				source = args[0];
				if (source != "b" && source != "y" && source != "c" && source != "e"
					   && source != "i" && source != "w" && source != "p")
				{
					WriteLine("sorry - I don't recognise that source argument");
					return;
				}
				else
				{
					if (args.Length == 2)
					{
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

					// proceed with one or two valid aprameters
    				int source_id = 0; string database_name = ""; string source_folder_name = "";
					switch (source)
					{
						case "b":
							{
								database_name = "biolincc";
								source_id = 101900;
								break;
							}
						case "y":
							{
								database_name = "yoda";
								source_id = 101901;
								break;
							}
						case "c":
							{
								database_name = "ctg";
								source_id = 100120;
								break;
							}
						case "e":
							{
								database_name = "euctr";
								source_id = 100123;
								break;
							}
						case "i":
							{
								database_name = "isrctn";
								source_id = 100126;
								break;
							}
						case "w":
							{
								database_name = "who";
								source_id = 100115;
								break;
							}
						case "p":
							{
								database_name = "pubmed";
								source_id = 100135;
								break;
							}
					}
					DataLayer repo = new DataLayer(database_name);
					source_folder_name = @"C\Data\" + database_name + @"\";

					Controller harvest_controller = new Controller(repo, source_id, source_folder_name, harvest_type_id);
					harvest_controller.EstablishNewSDTables();
					harvest_controller.LoopThroughFiles();
					harvest_controller.UpdateIds();
					harvest_controller.InsertHashes();
				}
			}
		}
	}
}
