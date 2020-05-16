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


		// get listing of local file paths
		public IEnumerable<string> FetchFilePaths(int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(_mon_connString))
			{
				string sql_string = "select local_path ";
				sql_string += " from sf.source_data_studies ";
				sql_string += " where source_id = " + source_id.ToString();
				sql_string += " and local_path is not null";
				sql_string += " order by local_path";
				return Conn.Query<string>(sql_string);
			}
		}

		// get record of interest
		public FileRecord FetchFileRecord(string sd_id, int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(_mon_connString))
			{
				string sql_string = "select id, source_id, sd_id, remote_url, last_sf_id, last_revised, ";
				sql_string += " assume_complete, download_status, download_datetime, local_path ";
				sql_string += " from sf.source_data_studies ";
				sql_string += " where sd_id = '" + sd_id + "' and source_id = " + source_id.ToString();
				return Conn.Query<FileRecord>(sql_string).FirstOrDefault();
			}
		}

		public void DeleteSDStudyTables()
		{
			StudyTableDroppers dropper = new StudyTableDroppers(yoda_connString);
			dropper.drop_table_studies();
			dropper.drop_table_study_identifiers();
			dropper.drop_table_study_titles();
			dropper.drop_table_study_contributors();
			dropper.drop_table_study_topics();
			dropper.drop_table_study_relationships();
			dropper.drop_table_study_references();
			dropper.drop_table_study_hashes();
		}

		public void DeleteSDObjectTables()
		{
			ObjectTableDroppers dropper = new ObjectTableDroppers(yoda_connString);
			dropper.drop_table_data_objects();
			dropper.drop_table_dataset_properties();
			dropper.drop_table_object_dates();
			dropper.drop_table_object_instances();
			dropper.drop_table_object_titles();
			dropper.drop_table_object_hashes();
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
			hashcreator.CreateObjectTitledHashes();

			ObjectHashInserters hashinserter = new ObjectHashInserters(yoda_connString);
			hashinserter.InsertStudyHashesIntoDataObjects();
			hashinserter.InsertObjectHashesIntoDatasetProperties();
			hashinserter.InsertObjectHashesIntoObjectInstances();
			hashinserter.InsertObjectHashesIntoObjectTitles();
		}

		public void CreateObjectCompositeHashes()
		{
			ObjectCompositeHashCreators hashcreator = new ObjectCompositeHashCreators(yoda_connString);
			hashcreator.CreateCompositeDatasetPropertiesHashes();
			hashcreator.CreateCompositeObjectInstanceHashes();
			hashcreator.CreateCompositeObjectTitlesHashes();
			hashcreator.CreateCompositeObjectDatesHashes();

			// objects must fully rolled up first..
			hashcreator.CreateFullDataObjectHashes();

			StudyCompositeHashCreators studyhashcreator = new StudyCompositeHashCreators(yoda_connString);
			studyhashcreator.CreateCompositeDataObjectHashes();
			studyhashcreator.CreateFullStudyHashes();
		}
	}

}
