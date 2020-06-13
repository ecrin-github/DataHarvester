using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;

namespace DataHarvester
{
	public class LoggingDataLayer
	{
		private string mon_connString;
		
		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		/// 
		public LoggingDataLayer()
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
			mon_connString = builder.ConnectionString;

			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
		}


		public IEnumerable<FileRecord> FetchStudyFileRecords(int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
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
			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
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
			using (var conn = new NpgsqlConnection(mon_connString))
			{
				return conn.Update<FileRecord>(file_record);
			}
		}


		public int InsertStudyFileRec(FileRecord file_record)
		{
			using (var conn = new NpgsqlConnection(mon_connString))
			{
				return (int)conn.Insert<FileRecord>(file_record);
			}
		}


		public void UpdateStudyFileRecLastProcessed(int id)
		{
			using (var conn = new NpgsqlConnection(mon_connString))
			{
				string sql_string = "update sf.source_data_studies";
				sql_string += " set last_processed = current_timestamp";
				sql_string += " where id = " + id.ToString();
				conn.Execute(sql_string);
			}
		}


	}
}

