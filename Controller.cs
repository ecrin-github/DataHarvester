using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml.Serialization;
using DataHarvester.BioLincc;
using DataHarvester.Yoda;
using DataHarvester.DBHelpers;

namespace DataHarvester
{
	public class Controller
	{
		DataLayer repo;
		LoggingDataLayer logging_repo;
		int harvest_type_id;
		int source_id;

		public Controller(int _source_id, int _harvest_type_id)
		{
			source_id = _source_id;
			repo = new DataLayer(source_id);
			logging_repo = new LoggingDataLayer();
			harvest_type_id = _harvest_type_id;
		}

		public void EstablishNewSDTables()
		{
			// do study tables unless pubmed, 
			// which is data objects only
			SDBuilder sdb = new SDBuilder(repo.ConnString, repo.SourceParameters);

			if (source_id != 100135)
			{
				sdb.DeleteSDStudyTables();
				sdb.BuildNewSDStudyTables();
			}
			sdb.DeleteSDObjectTables();
			sdb.BuildNewSDObjectTables();
		}


		public void LoopThroughFiles()
		{

			switch (source_id)
			{
				case 101900:
					{
						BioLinccController c = new BioLinccController(source_id, repo, logging_repo);
						c.LoopThroughFiles();
						break;
					}
				case 101901:
					{
						YodaController c = new YodaController(source_id, repo, logging_repo);
						c.LoopThroughFiles();
						break;
					}
				case 100120:
					{
						source_id = 100120;
						break;
					}
				case 100123:
					{
						source_id = 100123;
						break;
					}
				case 100126:
					{
						source_id = 100126;
						break;
					}
				case 100115:
					{
						source_id = 100115;
						break;
					}
				case 100135:
					{
						source_id = 100135;
						break;
					}
			}
		}


		public void CompleteSDTables()
		{
			HashBuilder hb = new HashBuilder(repo.ConnString, repo.SourceParameters);
			hb.UpdateStudyIdentifierOrgs();
			hb.UpdateDataObjectOrgs();
			hb.CreateStudyHashes();
			hb.CreateStudyCompositeHashes();
			hb.CreateDataObjectHashes();
			hb.CreateObjectCompositeHashes();
		}

	}

}
