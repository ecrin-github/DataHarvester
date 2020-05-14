using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;
using DataHarvester.DBHelpers;

namespace DataHarvester.BioLincc
{
	public class BioLinccDataLayer
	{
		private string _mon_connString;
		private string _biolincc_connString;
		private string _biolincc_pp_connString;
		private string _ctg_connString;
		private int source_id;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public BioLinccDataLayer(int _source_id)
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

			builder.Database = "biolincc";
			builder.SearchPath = "sd";
			_biolincc_connString = builder.ConnectionString;

			builder.Database = "biolincc";
			builder.SearchPath = "pp";
			_biolincc_pp_connString = builder.ConnectionString;

			builder.Database = "ctg";
			builder.SearchPath = "ad";
			_ctg_connString = builder.ConnectionString; 

			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
		}

		public void DeleteSDStudyTables()
		{
			StudyTableDroppers.drop_table_studies(_biolincc_connString);
			StudyTableDroppers.drop_table_study_identifiers(_biolincc_connString);
			StudyTableDroppers.drop_table_study_titles(_biolincc_connString);
			StudyTableDroppers.drop_table_study_relationships(_biolincc_connString);
			StudyTableDroppers.drop_table_study_references(_biolincc_connString);
			StudyTableDroppers.drop_table_study_jsonb(_biolincc_connString);
		}

		public void DeleteSDObjectTables()
		{
			ObjectTableDroppers.drop_table_data_objects(_biolincc_connString);
			ObjectTableDroppers.drop_table_dataset_properties(_biolincc_connString);
			ObjectTableDroppers.drop_table_object_dates(_biolincc_connString);
			ObjectTableDroppers.drop_table_object_instances(_biolincc_connString);
			ObjectTableDroppers.drop_table_object_titles(_biolincc_connString);
			ObjectTableDroppers.drop_table_object_jsonb(_biolincc_connString);
		}

		public void BuildNewSDStudyTables()
		{
			StudyTableBuildersSD.create_table_studies(_biolincc_connString);
			StudyTableBuildersSD.create_table_study_identifiers(_biolincc_connString);
			StudyTableBuildersSD.create_table_study_relationships(_biolincc_connString);
			StudyTableBuildersSD.create_table_study_references(_biolincc_connString);
			StudyTableBuildersSD.create_table_study_titles(_biolincc_connString);
			StudyTableBuildersSD.create_table_study_hashes(_biolincc_connString);
		}


		public void BuildNewSDObjectTables()
		{
			ObjectTableBuildersSD.create_table_data_objects(_biolincc_connString);
			ObjectTableBuildersSD.create_table_dataset_properties(_biolincc_connString);
			ObjectTableBuildersSD.create_table_object_dates(_biolincc_connString);
			ObjectTableBuildersSD.create_table_object_instances(_biolincc_connString);
			ObjectTableBuildersSD.create_table_object_titles(_biolincc_connString);
			ObjectTableBuildersSD.create_table_object_hashes(_biolincc_connString);
		}
			

		public ObjectTypeDetails FetchDocTypeDetails(string doc_name)
		{
			using (var conn = new NpgsqlConnection(_biolincc_pp_connString))
			{
				string sql_string = "Select type_id, type_name from pp.document_types ";
				sql_string += "where resource_name = '" + doc_name + "';";
				return conn.QueryFirstOrDefault<ObjectTypeDetails>(sql_string);
			}
		}


		public SponsorDetails FetchBioLINCCSponsorFromNCT(string nct_id)
		{
			using (var conn = new NpgsqlConnection(_ctg_connString))
			{
				string sql_string = "Select organisation_id as org_id, organisation_name as org_name from ad.study_contributors ";
				sql_string += "where sd_id = '" + nct_id + "' and contrib_type_id = 54;";
				return conn.QueryFirstOrDefault<SponsorDetails>(sql_string);
			}
		}

		public string FetchStudyTitle(string nct_id)
		{
			using (var conn = new NpgsqlConnection(_ctg_connString))
			{
				string sql_string = "Select display_title  from ad.studies ";
				sql_string += "where sd_id = '" + nct_id + "'";
				return conn.QueryFirstOrDefault<string>(sql_string);
			}
		}


		public void StoreStudy(StudyInDB st_db)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Insert<StudyInDB>(st_db);
			}
		}

		public ulong StoreStudyIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper, IEnumerable<StudyIdentifier> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper, IEnumerable<StudyTitle> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper, IEnumerable<StudyRelationship> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}

		}
		public ulong StoreStudyReferences(PostgreSQLCopyHelper<StudyReference> copyHelper, IEnumerable<StudyReference> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper, IEnumerable<DataObject> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreDatasetProperties(PostgreSQLCopyHelper<DataSetProperties> copyHelper, IEnumerable<DataSetProperties> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectTitles(PostgreSQLCopyHelper<DataObjectTitle> copyHelper,
						IEnumerable<DataObjectTitle> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectDates(PostgreSQLCopyHelper<DataObjectDate> copyHelper,
						IEnumerable<DataObjectDate> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectInstances(PostgreSQLCopyHelper<DataObjectInstance> copyHelper,
						IEnumerable<DataObjectInstance> entities)
		{
			using (var conn = new NpgsqlConnection(_biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public void UpdateStudyIdentifierOrgs()
		{
			OrgHelper.update_study_identifiers_using_default_name(_biolincc_connString);
			OrgHelper.update_study_identifiers_using_other_name(_biolincc_connString);
			OrgHelper.update_study_identifiers_insert_default_names(_biolincc_connString);
		}

		public void UpdateDataObjectOrgs()
		{
			OrgHelper.update_data_objects_using_default_name(_biolincc_connString);
			OrgHelper.update_data_objects_using_other_name(_biolincc_connString);
			OrgHelper.update_data_objects_insert_default_names(_biolincc_connString);
		}

		public void CreateStudyHashes()
		{
			StudyHashCreators.CreateStudyIdHashes(_biolincc_connString, source_id);

			StudyHashCreators.CreateStudyRecordHashes(_biolincc_connString);
			StudyHashCreators.CreateStudyIdentifierHashes(_biolincc_connString);
			StudyHashCreators.CreateStudyTitleHashes(_biolincc_connString);
			StudyHashCreators.CreateStudyReferenceHashes(_biolincc_connString);

			StudyHashInserters.InsertStudyHashesIntoStudyIdentifiers(_biolincc_connString);
			StudyHashInserters.InsertStudyHashesIntoStudyTitles(_biolincc_connString);
			StudyHashInserters.InsertStudyHashesIntoStudyReferences(_biolincc_connString);
		}

		public void CreateStudyCompositeHashes()
		{
			StudyCompositeHashCreators.CreateCompositeStudyIdentifierHashes(_biolincc_connString);
			StudyCompositeHashCreators.CreateCompositeStudyTitleHashes(_biolincc_connString);
			StudyCompositeHashCreators.CreateCompositeStudyReferenceHashes(_biolincc_connString);
		}

		public void CreateDataObjectHashes()
		{
			ObjectHashCreators.CreateObjectIdHashes(_biolincc_connString);

			ObjectHashCreators.CreateObjectRecordHashes(_biolincc_connString);
			ObjectHashCreators.CreateRecordsetPropertiesHashes(_biolincc_connString);
			ObjectHashCreators.CreateObjectInstanceHashes(_biolincc_connString);
			ObjectHashCreators.CreateObjectTitledHashes(_biolincc_connString);
			ObjectHashCreators.CreateObjectDateHashes(_biolincc_connString);

			ObjectHashCreators.InsertStudyHashesIntoDataObjects(_biolincc_connString);
			ObjectHashCreators.InsertObjectHashesIntoDatasetProperties(_biolincc_connString);
			ObjectHashCreators.InsertObjectHashesIntoObjectInstances(_biolincc_connString);
			ObjectHashCreators.InsertObjectHashesIntoObjectTitles(_biolincc_connString);
			ObjectHashCreators.InsertObjectHashesIntoObjectDates(_biolincc_connString);
		}

		public void CreateObjectCompositeHashes()
		{
			ObjectCompositeHashCreators.CreateCompositeObjectInstanceHashes(_biolincc_connString);
			ObjectCompositeHashCreators.CreateCompositeObjectTitlesHashes(_biolincc_connString);
			ObjectCompositeHashCreators.CreateCompositeObjectDatesHashes(_biolincc_connString);
		}

		public void CreateRollUpHashes()
		{
			// objects must be done first..
			ObjectCompositeHashCreators.CreateFullDataObjectHashes(_biolincc_connString);
			StudyCompositeHashCreators.CreateCompositeDataObjectHashes(_biolincc_connString);
			StudyCompositeHashCreators.CreateFullStudyHashes(_biolincc_connString);
		}
	}
}
