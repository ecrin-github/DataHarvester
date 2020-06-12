using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using DataHarvester.DBHelpers;
using PostgreSQLCopyHelper;

namespace DataHarvester.Yoda
{
	public class YodaDataLayer
	{
		private string _mon_connString;
		private string _ctg_connString;
		private string _isrctn_connString;
		private string yoda_connString;
		private int source_id;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public YodaDataLayer(int _source_id)
		{
			source_id = _source_id;

			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			builder.Database = "mon";
			builder.SearchPath = "sf";
			_mon_connString = builder.ConnectionString;

			builder.Database = "ctg";
			builder.SearchPath = "ad";
			_ctg_connString = builder.ConnectionString;

			builder.Database = "isrctn";
			builder.SearchPath = "ad";
			_isrctn_connString = builder.ConnectionString;

			builder.Database = "yoda";
			builder.SearchPath = "sd";
			yoda_connString = builder.ConnectionString;

			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
		}
		
		public void BuildNewSDStudyTables()
		{
			StudyTableBuildersSD builder = new StudyTableBuildersSD(yoda_connString);
			builder.create_table_studies();
			builder.create_table_study_identifiers();
			builder.create_table_study_titles();
			builder.create_table_study_topics();
			builder.create_table_study_contributors();
			builder.create_table_study_relationships();
			builder.create_table_study_references();
			builder.create_table_study_hashes();
		}


		public void BuildNewSDObjectTables()
		{
			ObjectTableBuildersSD builder = new ObjectTableBuildersSD(yoda_connString);
			builder.create_table_data_objects();
			builder.create_table_dataset_properties();
			builder.create_table_object_dates();
			builder.create_table_object_instances();
			builder.create_table_object_titles();
			builder.create_table_object_languages();
			builder.create_table_object_hashes();
		}


		public void StoreStudy(StudyInDB st_db)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Insert<StudyInDB>(st_db);
			}
		}

		public ulong StoreStudyIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper, IEnumerable<StudyIdentifier> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper, IEnumerable<StudyTitle> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyContributors(PostgreSQLCopyHelper<StudyContributor> copyHelper, IEnumerable<StudyContributor> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyTopics(PostgreSQLCopyHelper<StudyTopic> copyHelper, IEnumerable<StudyTopic> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

	    public ulong StoreStudyRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper, IEnumerable<StudyRelationship> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}

		}
		public ulong StoreStudyReferences(PostgreSQLCopyHelper<StudyReference> copyHelper, IEnumerable<StudyReference> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper, IEnumerable<DataObject> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreDatasetProperties(PostgreSQLCopyHelper<DataSetProperties> copyHelper, IEnumerable<DataSetProperties> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectTitles(PostgreSQLCopyHelper<DataObjectTitle> copyHelper,
						IEnumerable<DataObjectTitle> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectDates(PostgreSQLCopyHelper<DataObjectDate> copyHelper,
						IEnumerable<DataObjectDate> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectInstances(PostgreSQLCopyHelper<DataObjectInstance> copyHelper,
						IEnumerable<DataObjectInstance> entities)
		{
			using (var conn = new NpgsqlConnection(yoda_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public void StoreObjectLanguages()
		{
			// inserts a default 'en' language record for each data object
			LanguageDataHelper helper = new LanguageDataHelper(yoda_connString);
			helper.CreateDefaultLanguageData();
		}

		public void UpdateStudyIdentifierOrgs()
		{
			OrgHelper helper = new OrgHelper(yoda_connString);
			helper.update_study_identifiers_using_default_name();
			helper.update_study_identifiers_using_other_name();
			helper.update_study_identifiers_insert_default_names();
		}

		public void UpdateDataObjectOrgs()
		{
			OrgHelper helper = new OrgHelper(yoda_connString);
			helper.update_data_objects_using_default_name();
			helper.update_data_objects_using_other_name();
			helper.update_data_objects_insert_default_names();
		}

		public void CreateStudyHashes()
		{

			StudyHashCreators hashcreator = new StudyHashCreators(yoda_connString);
			hashcreator.CreateStudyIdHashes(source_id);
			hashcreator.CreateStudyRecordHashes();

			hashcreator.CreateStudyIdentifierHashes();
			hashcreator.CreateStudyTitleHashes();
			hashcreator.CreateStudyContributorHashes();
			hashcreator.CreateStudyTopicHashes();
			hashcreator.CreateStudyReferenceHashes();

			StudyHashInserters hashinserter = new StudyHashInserters(yoda_connString);
			hashinserter.InsertStudyHashesIntoStudyIdentifiers();
			hashinserter.InsertStudyHashesIntoStudyTitles();
			hashinserter.InsertStudyHashesIntoStudyContributors();
			hashinserter.InsertStudyHashesIntoStudyTopics();
			hashinserter.InsertStudyHashesIntoStudyReferences();
		}

		public void CreateStudyCompositeHashes()
		{
			StudyCompositeHashCreators hashcreator = new StudyCompositeHashCreators(yoda_connString);
			hashcreator.CreateCompositeStudyIdentifierHashes();
			hashcreator.CreateCompositeStudyTitleHashes();
			hashcreator.CreateCompositeStudyContributorHashes();
			hashcreator.CreateCompositeStudyTopicHashes();
			hashcreator.CreateCompositeStudyReferenceHashes();
		}

		public void CreateDataObjectHashes()
		{
			ObjectHashCreators hashcreator = new ObjectHashCreators(yoda_connString);
			hashcreator.CreateObjectIdHashes();
			hashcreator.CreateObjectRecordHashes();
			hashcreator.CreateRecordsetPropertiesHashes();
			hashcreator.CreateObjectInstanceHashes();
			hashcreator.CreateObjectTitleHashes();
			hashcreator.CreateObjectLanguageHashes();

			ObjectHashInserters hashinserter = new ObjectHashInserters(yoda_connString);
			hashinserter.InsertStudyHashesIntoDataObjects();
			hashinserter.InsertObjectHashesIntoDatasetProperties();
			hashinserter.InsertObjectHashesIntoObjectInstances();
			hashinserter.InsertObjectHashesIntoObjectTitles();
			hashinserter.InsertObjectHashesIntoObjectLanguages();
		}

		public void CreateObjectCompositeHashes()
		{
			ObjectCompositeHashCreators hashcreator = new ObjectCompositeHashCreators(yoda_connString);
			hashcreator.CreateCompositeDatasetPropertiesHashes();
			hashcreator.CreateCompositeObjectInstanceHashes();
			hashcreator.CreateCompositeObjectTitlesHashes();
			hashcreator.CreateCompositeObjectDatesHashes();
			hashcreator.CreateCompositeObjectLanguagesHashes();

			// objects must fully rolled up first..
			hashcreator.CreateFullDataObjectHashes();

			StudyCompositeHashCreators studyhashcreator = new StudyCompositeHashCreators(yoda_connString);
			studyhashcreator.CreateCompositeDataObjectHashes();
			studyhashcreator.CreateFullStudyHashes();
		}
	}

}
