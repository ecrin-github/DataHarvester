using System;
using System.IO;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataHarvester.ctg
{
	class CTGController
	{
		DataLayer common_repo;
		LoggingDataLayer logging_repo;
		CTGProcessor processor;
		Source source;

		public CTGController(Source _source, DataLayer _common_repo, LoggingDataLayer _logging_repo)
		{
			source = _source;
			processor = new CTGProcessor();
			common_repo = _common_repo;
			logging_repo = _logging_repo;
		}

		public async Task LoopThroughFilesAsync()
		{
			URLChecker checker = new URLChecker();

			// Get the folder base from the appsettings file
			// and construct a list of the folders of CGT XML files

			string folderBase = source.local_folder;
			string[] folder_list = Directory.GetDirectories(folderBase);

			for (int f = 0; f < folder_list.Length; f++)
			{
				// for testing...
				// if (f == 5) break;
				string fileBase = folder_list[f];

				// Get the files within each folder in turn.
				string[] file_list = Directory.GetFiles(fileBase);
				for (int g = 0; g < file_list.Length; g++)
				{
					// for testing...
					//if (g > 5) break; 
					string fileName = file_list[g];

					if (File.Exists(fileName))
					{
						string inputString = "";
						using (var streamReader = new StreamReader(fileName, System.Text.Encoding.UTF8))
						{
							inputString += streamReader.ReadToEnd();
						}

						XmlSerializer serializer = new XmlSerializer(typeof(FullStudy));
						StringReader rdr = new StringReader(inputString);
						FullStudy studyRegEntry = (FullStudy)serializer.Deserialize(rdr);

						// break up the file into relevant data classes
						DateTime? download_time = null; // **********to SORT
						Study s = processor.ProcessData(studyRegEntry, download_time, common_repo);

						// check and store data object links - just pdfs for now
						// (commented out for the moment to save time during extraction).
						await checker.CheckURLsAsync(s.object_instances);

						// store the data in the database
						processor.StoreData(common_repo, s);

					}

				}
				
				Console.WriteLine(folder_list[f]); 
			}
		}
	}
}
