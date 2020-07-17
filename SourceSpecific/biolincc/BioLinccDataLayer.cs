using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;

namespace DataHarvester.biolincc
{
	public class BioLinccDataLayer
	{
		private string bio_connString;
		private string ctg_connString;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// </summary>
		/// 
		public BioLinccDataLayer()
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


		public SponsorDetails FetchBioLINCCSponsorFromNCT(string nct_connString, string nct_id)
		{
			using (var conn = new NpgsqlConnection(nct_connString))
			{
				string sql_string = "Select organisation_id as org_id, organisation_name as org_name from ad.study_contributors ";
				sql_string += "where sd_id = '" + nct_id + "' and contrib_type_id = 54;";
				return conn.QueryFirstOrDefault<SponsorDetails>(sql_string);
			}
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


		public void CreateMultNCTsTable()
		{
			string sql_string = @"Drop table if exists pp.multiple_ncts;
             create table pp.multiple_ncts as 
			select si.sd_sid, si.identifier_value
			from sd.study_identifiers si inner join
			    (select sd_sid, count(identifier_value)
			     from sd.study_identifiers
			     where identifier_org_id = 100120
			     group by sd_sid
			     having count(identifier_value) > 1) mults
             on si.sd_sid = mults.sd_sid
			 where si.identifier_org_id = 100120;";

			using (var conn = new NpgsqlConnection(bio_connString))
			{
				conn.Execute(sql_string);
			}
		}


		public void CreateMultHBLIsTable()
		{

			string sql_string = @"Drop table if exists pp.multiple_hlbis;
            create table pp.multiple_hlbis as
			select si.sd_sid, si.identifier_value 
			from sd.study_identifiers si inner join
			   (select identifier_value, count(sd_sid)
			    from sd.study_identifiers
			    where identifier_org_id = 100120
			    group by identifier_value
			    having count(sd_sid) > 1) ncts
			on si.identifier_value = ncts.identifier_value;";

			using (var conn = new NpgsqlConnection(bio_connString))
			{
				conn.Execute(sql_string);
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

