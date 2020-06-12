using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using DataHarvester.DBHelpers;

namespace DataHarvester
{
	public class DataLayer
	{
		private string connString;
		private string _biolincc_pp_connString;
		private string _ctg_connString;
		private string _isrctn_connString;
		private string _biolincc_folder_base;
		private string _yoda_folder_base;
		private string _yoda_pp_connString;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public DataLayer(string database_name)
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			builder.Database = database_name;
			connString = builder.ConnectionString;
			
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
			StudyTableDroppers dropper = new StudyTableDroppers(connString);
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
			ObjectTableDroppers dropper = new ObjectTableDroppers(connString);
			dropper.drop_table_data_objects();
			dropper.drop_table_dataset_properties();
			dropper.drop_table_object_dates();
			dropper.drop_table_object_instances();
			dropper.drop_table_object_titles();
			dropper.drop_table_object_languages();
			dropper.drop_table_object_hashes();
		}

		public void BuildNewSDStudyTables()
		{
			StudyTableBuildersSD builder = new StudyTableBuildersSD(connString);
			builder.create_table_studies();
			builder.create_table_study_identifiers();
			builder.create_table_study_relationships();
			builder.create_table_study_references();
			builder.create_table_study_titles();
			builder.create_table_study_hashes();
		}


		public void BuildNewSDObjectTables()
		{
			ObjectTableBuildersSD builder = new ObjectTableBuildersSD(connString);
			builder.create_table_data_objects();
			builder.create_table_dataset_properties();
			builder.create_table_object_dates();
			builder.create_table_object_instances();
			builder.create_table_object_titles();
			builder.create_table_object_languages();
			builder.create_table_object_hashes();
		}








		public string GetBioLinccFolderBase() => _biolincc_folder_base;

		public string GetYodaFolderBase() => _yoda_folder_base;




		public IEnumerable<FileRecord> FetchStudyFileRecords(int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
			{
				string sql_string = "select id, source_id, sd_id, remote_url, last_sf_id, last_revised, ";
				sql_string += " assume_complete, download_status, download_datetime, local_path, last_processed ";
				sql_string += " from sf.source_data_studies ";
				sql_string += " where source_id = " + source_id.ToString();
				sql_string += " and local_path is not null";
				sql_string += " order by local_path";
				return Conn.Query<FileRecord>(sql_string);
			}
		}


		// get record of interest
		public FileRecord FetchStudyFileRecord(string sd_id, int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
			{
				string sql_string = "select id, source_id, sd_id, remote_url, last_sf_id, last_revised, ";
				sql_string += " assume_complete, download_status, download_datetime, local_path, last_processed ";
				sql_string += " from sf.source_data_studies ";
				sql_string += " where sd_id = '" + sd_id + "' and source_id = " + source_id.ToString();
				return Conn.Query<FileRecord>(sql_string).FirstOrDefault();
			}
		}

		public bool StoreStudyFileRec(FileRecord file_record)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				return conn.Update<FileRecord>(file_record);
			}
		}


		public int InsertStudyFileRec(FileRecord file_record)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				return (int)conn.Insert<FileRecord>(file_record);
			}
		}


		public void UpdateStudyFileRecLastProcessed(int id)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				string sql_string = "update sf.source_data_studies";
				sql_string += " set last_processed = current_timestamp";
				sql_string += " where id = " + id.ToString();
				conn.Execute(sql_string);
			}
		}

	}
}
