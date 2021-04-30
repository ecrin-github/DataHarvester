using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Linq;

namespace DataHarvester.biolincc
{
    public class BioLinccDataLayer
    {
        private string bio_connString;
        private string nct_connString;

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

            builder.Database = "cgt";
            nct_connString = builder.ConnectionString;

        }


        public SponsorDetails FetchBioLINCCSponsorFromNCT(string nct_id)
        {
            using (var conn = new NpgsqlConnection(nct_connString))
            {
                string sql_string = "Select organisation_id as org_id, organisation_name as org_name from ad.study_contributors ";
                sql_string += "where sd_sid = '" + nct_id + "' and contrib_type_id = 54;";
                return conn.QueryFirstOrDefault<SponsorDetails>(sql_string);
            }
        }


        public string FetchStudyTitle(string nct_id)
        {
            using (var conn = new NpgsqlConnection(nct_connString))
            {
                string sql_string = "Select display_title  from ad.studies ";
                sql_string += "where sd_sid = '" + nct_id + "'";
                return conn.QueryFirstOrDefault<string>(sql_string);
            }
        }


        public void CreateMultNCTsTable()
        {
            // inner subquery identifies biolincc records 
            // that have more than one linked NCT identifier
            
            string sql_string = @"Drop table if exists pp.multiple_ncts;
            create table pp.multiple_ncts as 
            select si.sd_sid, si.identifier_value
            from sd.study_identifiers si inner join
                (select sd_sid
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
            // inner subquery identifies NCT identifiers 
            // that are linked to more than one Biolinnc studies

            string sql_string = @"Drop table if exists pp.multiple_hlbis;
            create table pp.multiple_hlbis as
            select si.sd_sid, si.identifier_value 
            from sd.study_identifiers si inner join
               (select identifier_value
                from sd.study_identifiers
                where identifier_org_id = 100120
                group by identifier_value
                having count(sd_sid) > 1) ncts
            on si.identifier_value = ncts.identifier_value
            where si.identifier_org_id = 100120;";

            using (var conn = new NpgsqlConnection(bio_connString))
            {
                conn.Execute(sql_string);
            }
        }

        public string FetchFirstNCTId(string sid)
        {
            string sql_string = @"select identifier_value from sd.study_identifiers 
                                  where sd_sid = '" + sid + @"' and 
                                  identifier_org_id = 100120;";

            using (var conn = new NpgsqlConnection(bio_connString))
            {
                return conn.Query<string>(sql_string).FirstOrDefault(); 
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

