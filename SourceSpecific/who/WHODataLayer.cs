using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Linq;

namespace DataHarvester.who
{
    public class WHODataLayer
	{
		private string bio_connString;
		private string ctg_connString;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// </summary>
		/// 
		public WHODataLayer()
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			builder.Database = "biolincc";
			bio_connString = builder.ConnectionString;

			builder.Database = "ctg";
			ctg_connString = builder.ConnectionString;

		}


		public string FetchStudyTitle(string nct_connString, string nct_id)
		{
			using (var conn = new NpgsqlConnection(nct_connString))
			{
				string sql_string = "Select display_title  from ad.studies ";
				sql_string += "where sd_id = '" + nct_id + "'";
				return conn.QueryFirstOrDefault<string>(sql_string);
			}
		}


		public bool InMultipleHBLIGroup(string sid)
        {
			string sql_string = @"select sd_sid from pp.multiple_hlbis 
                                  where sd_sid = '" + sid + "'";

			using (var conn = new NpgsqlConnection(bio_connString))
			{
				int res = conn.Query<string>(sql_string).Count();
				return (res > 0) ? true : false;
			}

		}
	}
}

