using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;

namespace DataHarvester
{
	public class DataLayer
	{
		private string _connString;
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
		public DataLayer()
		{
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
			_connString = builder.ConnectionString;

			builder.Database = "biolincc";
			builder.SearchPath = "pp";
			_biolincc_pp_connString = builder.ConnectionString;

			builder.Database = "ctg";
			builder.SearchPath = "ad";
			_ctg_connString = builder.ConnectionString;

			builder.Database = "isrctn";
			builder.SearchPath = "ad";
			_isrctn_connString = builder.ConnectionString;

			builder.Database = "yoda";
			builder.SearchPath = "pp";
			_yoda_pp_connString = builder.ConnectionString;

			_biolincc_folder_base = settings["biolincc_folder_base"];
			_yoda_folder_base = settings["yoda_folder_base"];

			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
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
