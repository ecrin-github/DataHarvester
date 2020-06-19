using System.Collections.Generic;
using System.Data;
using Npgsql;
using Dapper;
using Dapper.Contrib.Extensions;
using System;
using PostgreSQLCopyHelper;
using Microsoft.Extensions.Configuration;

namespace DataHarvester.pubmed
{
    public class PubmedDataLayer
    {
        private string pubmed_connString;
        private string mon_sf_connString;

        
        // Parameterless constructor is used to automatically build
        // the connection string, using an appsettings.json file that 
        // has the relevant credentials (but which is not stored in GitHub).
        // The json file also includes the root folder path, which is
        // stored in the class's folder_base property.

        public PubmedDataLayer()
        {
            IConfigurationRoot settings = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = settings["host"];
            builder.Username = settings["user"];
            builder.Password = settings["password"];

            builder.Database = "pubmed";
            builder.SearchPath = "sd";
            pubmed_connString = builder.ConnectionString;

            builder.Database = "mon";
            builder.SearchPath = "sf";
            mon_sf_connString = builder.ConnectionString;

        }


        // Brings back the total number of pmids / pubmed files to process

        public long Get_pmid_record_count()
        {
            using (IDbConnection Conn = new NpgsqlConnection(mon_sf_connString))
            {
                string sql_string = "select count(*) from sf.source_data_objects where last_processed is null";
                return (long)Conn.ExecuteScalar(sql_string);
            }
        }

        // Brings back the integer IDs (pmids) and file paths of the records of interest
        // 10,000 at a time

        public IEnumerable<ObjectFileRecord> Get_pmid_records(int skip)
        {
            using (IDbConnection Conn = new NpgsqlConnection(mon_sf_connString))
            {
                string sql_string = "select id, sd_id, local_path from sf.source_data_objects ";
                sql_string += " where last_processed is null order by sd_id limit 10000 offset " + (skip * 10000).ToString();
                return Conn.Query<ObjectFileRecord>(sql_string);
            }
        }


        public void UpdateFileRecord(int id)
        {
            string sql_string = @"UPDATE sf.source_data_objects 
                          SET last_processed = current_timestamp
                          where id = " + id.ToString();

            using (IDbConnection Conn = new NpgsqlConnection(mon_sf_connString))
            {
                Conn.Execute(sql_string);
            }
        }


        // Stores an 'extraction note', e.g. an unusual occurence found and
        // logged during the extraction, in the associated table.

        public void StoreExtractionNote(string id, int note_type, string note)
        {
            ExtractionNote en = new ExtractionNote(id, note_type, note);
            using (IDbConnection Conn = new NpgsqlConnection(pubmed_connString))
            {
                Conn.Insert<ExtractionNote>(en);
            }
        }


        public bool FileInDatabase(string sd_oid)
        {
            string sql_string = "SELECT count(*) FROM sd.data_objects";
            sql_string += " where sd_oid = " + sd_oid;
            using (IDbConnection Conn = new NpgsqlConnection(pubmed_connString))
            {
                long res = (long)Conn.ExecuteScalar(sql_string);
                return (res > 0);
            }
        }

    }


}
