using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
	class ImportBuilder
	{
		string connstring;
		Source source;
		ImportTableCreator itc;
		ImportTableManager itm;

		public ImportBuilder(string _connstring, Source _source)
		{
			connstring = _connstring;
			source = _source;
			itc = new ImportTableCreator(connstring);
			itm = new ImportTableManager(connstring);
		}

		public void CreateImportTables()
		{
			if (source.has_study_tables)
			{
				itc.CreateStudyRecsImportTable();
				itc.CreateStudyAttsImportTable();
				StringHelpers.SendFeedback("Created studies import tables");
			}
			itc.CreateObjectRecsImportTable();
			itc.CreateObjectAttsImportTable();
			StringHelpers.SendFeedback("Created data objects import tables");
		}


		public void FillImportTables(bool count_deleted)
		{
			itm.CheckExistanceOfADTables();
			if (source.has_study_tables)
			{
				itm.IdentifyNewStudies();
				itm.IdentifyIdenticalStudies();
				itm.IdentifyEditedStudies();
				if (count_deleted)
				{
					itm.IdentifyDeletedStudies();
				}
				itm.IdentifyChangedStudyRecs();
				itm.IdentifyChangedStudyAtts();
				itm.IdentifyNewStudyAtts();
				if (count_deleted)
				{
					itm.IdentifyDeletedStudyAtts();
				}
				StringHelpers.SendFeedback("Filled studies import tables");
			}

			itm.IdentifyNewDataObjects();
			itm.IdentifyIdenticalDataObjects();
			itm.IdentifyEditedDataObjects();
			if (count_deleted)
			{
				itm.IdentifyDeletedDataObjects();
			}
			itm.IdentifyChangedObjectRecs();
			if (source.has_dataset_properties)
			{
				itm.IdentifyChangedDatasetRecs();
			}
			itm.IdentifyChangedObjectAtts();
			itm.IdentifyNewObjectAtts();
			if (count_deleted)
			{
				itm.IdentifyDeletedObjectAtts();
			}
			StringHelpers.SendFeedback("Filled data objects import tables");
		}
	}
}
