using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using DataHarvester.DBHelpers;
using PostgreSQLCopyHelper;

namespace DataHarvester
{
	public class HashBuilder
	{
		private string connString;
		public HashBuilder(string _connString, Source source)
		{
			connString = _connString;
		}

		public void UpdateStudyIdentifierOrgs()
		{
			OrgHelper helper = new OrgHelper(connString);
			helper.update_study_identifiers_using_default_name();
			helper.update_study_identifiers_using_other_name();
			helper.update_study_identifiers_insert_default_names();
		}

		public void UpdateDataObjectOrgs()
		{
			OrgHelper helper = new OrgHelper(connString);
			helper.update_data_objects_using_default_name();
			helper.update_data_objects_using_other_name();
			helper.update_data_objects_insert_default_names();
		}

		public void CreateStudyHashes()
		{
			// TODO - amend as per source...

			StudyHashCreators hashcreator = new StudyHashCreators(connString);
			hashcreator.CreateStudyRecordHashes();
			hashcreator.CreateStudyIdentifierHashes();
			hashcreator.CreateStudyTitleHashes();
			hashcreator.CreateStudyReferenceHashes();

			StudyHashInserters hashinserter = new StudyHashInserters(connString);
			hashinserter.InsertStudyHashesIntoStudyIdentifiers();
			hashinserter.InsertStudyHashesIntoStudyTitles();
			hashinserter.InsertStudyHashesIntoStudyReferences();
		}

		public void CreateStudyCompositeHashes()
		{
			// TODO - amend as per source...
			
			StudyCompositeHashCreators hashcreator = new StudyCompositeHashCreators(connString);
			hashcreator.CreateCompositeStudyIdentifierHashes();
			hashcreator.CreateCompositeStudyTitleHashes();
			hashcreator.CreateCompositeStudyReferenceHashes();
		}

		public void CreateDataObjectHashes()
		{
			// TODO - amend as per source...
			
			ObjectHashCreators hashcreator = new ObjectHashCreators(connString);
			hashcreator.CreateObjectIdHashes();
			hashcreator.CreateObjectRecordHashes();
			hashcreator.CreateRecordsetPropertiesHashes();
			hashcreator.CreateObjectInstanceHashes();
			hashcreator.CreateObjectTitleHashes();
			hashcreator.CreateObjectDateHashes();
			hashcreator.CreateObjectLanguageHashes();

			ObjectHashInserters hashinserter = new ObjectHashInserters(connString);
			hashinserter.InsertStudyHashesIntoDataObjects();
			hashinserter.InsertObjectHashesIntoDatasetProperties();
			hashinserter.InsertObjectHashesIntoObjectInstances();
			hashinserter.InsertObjectHashesIntoObjectTitles();
			hashinserter.InsertObjectHashesIntoObjectDates();
			hashinserter.InsertObjectHashesIntoObjectLanguages();
		}

		public void CreateObjectCompositeHashes()
		{
			// TODO - amend as per source...
			
			ObjectCompositeHashCreators hashcreator = new ObjectCompositeHashCreators(connString);
			hashcreator.CreateCompositeDatasetPropertiesHashes();
			hashcreator.CreateCompositeObjectInstanceHashes();
			hashcreator.CreateCompositeObjectTitlesHashes();
			hashcreator.CreateCompositeObjectDatesHashes();
			hashcreator.CreateCompositeObjectLanguagesHashes();

			// objects must fully rolled up first..
			hashcreator.CreateFullDataObjectHashes();

			StudyCompositeHashCreators studyhashcreator = new StudyCompositeHashCreators(connString);
			studyhashcreator.CreateCompositeDataObjectHashes();
			studyhashcreator.CreateFullStudyHashes();
		}

	}
}

