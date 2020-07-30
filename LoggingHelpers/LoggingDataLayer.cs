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
		private Source source;


		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
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

		}

		public Source SourceParameters => source;

		public Source FetchSourceParameters(int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
			{
				source = Conn.Get<Source>(source_id);
				return source;
			}
		}

		/*
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
		*/

		public IEnumerable<FileRecord> FetchStudyFileRecords(int source_id, int harvest_type_id = 1, DateTime? cutoff_date = null)
		{
			string sql_string = "select id, source_id, sd_id, remote_url, last_sf_id, last_revised, ";
			sql_string += " assume_complete, download_status, download_datetime, local_path, last_processed ";
			sql_string += " from sf.source_data_studies ";

			if (harvest_type_id == 1)
            {
				// Harvest all files.
                sql_string += " where source_id = " + source_id.ToString();
                
            }
            else if (harvest_type_id == 2)
            {
				// Harvest only those files that have been revised (or added) on or since the cutoff date.

				sql_string += " where source_id = " + source_id.ToString() + " and last_revised >= '" + cutoff_date + "'";
			}
            else if (harvest_type_id == 3)
            {
				// For sources with no revision date - harvestfiles unless assumed complete has been set
				// as true (default is null) in which case no further change is expected.

				sql_string += " where source_id = " + source_id.ToString() + " and assume_complete is null";
			}
			
			sql_string += " and local_path is not null";
            sql_string += " order by local_path";

			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
			{
				return Conn.Query<FileRecord>(sql_string);
			}
		}


		public int FetchStudyFileRecordsCount(int source_id, int harvest_type_id = 1, DateTime? cutoff_date = null)
	    {
			string sql_string = "select count(*) ";
			sql_string += " from sf.source_data_studies ";
			if (harvest_type_id == 1)
			{
				// Count all files.

				sql_string += " where source_id = " + source_id.ToString();
			}
			else if (harvest_type_id == 2)
			{
				// Count only those files that have been revised (or added) on or since the cutoff date.

				sql_string += " where source_id = " + source_id.ToString() + " and last_revised >= '" + cutoff_date + "'";
			}
			else if (harvest_type_id == 3)
			{
				// For sources with no revision date - Count files unless assumed complete has been set
				// as true (default is null) in which case no further change is expected.

				sql_string += " where source_id = " + source_id.ToString() + " and assume_complete is null";
			}

			sql_string += " and local_path is not null";

			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
			{
				return Conn.ExecuteScalar<int>(sql_string);
			}
		}



        public IEnumerable<FileRecord> FetchStudyFileRecordsByOffset(int source_id, int offset_num, int amount, int harvest_type_id = 1, DateTime? cutoff_date = null)
        {
			string sql_string = "select id, source_id, sd_id, remote_url, last_sf_id, last_revised, ";
			sql_string += " assume_complete, download_status, download_datetime, local_path, last_processed ";
			sql_string += " from sf.source_data_studies ";

			if (harvest_type_id == 1)
			{
				// Harvest all files.
				sql_string += " where source_id = " + source_id.ToString();
			}
			else if (harvest_type_id == 2)
			{
				// Harvest only those files that have been revised (or added) on or since the cutoff date.

				sql_string += " where source_id = " + source_id.ToString() + " and last_revised >= '" + cutoff_date + "'";
			}
			else if (harvest_type_id == 3)
			{
				// For sources with no revision date - harvestfiles unless assumed complete has been set
				// as true (default is null) in which case no further change is expected.

				sql_string += " where source_id = " + source_id.ToString() + " and assume_complete is null";
			}

			sql_string += " and local_path is not null";
			sql_string += " order by local_path";
			sql_string += " offset " + offset_num.ToString() + " limit " + amount.ToString();

			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
			{
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

