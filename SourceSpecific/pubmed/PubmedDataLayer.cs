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
        private string pubmed_sd_connString;
        private string mon_sf_connString;
        private string pubmed_pp_connString;
        private string folder_base;

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
            pubmed_sd_connString = builder.ConnectionString;

            builder.Database = "mon";
            builder.SearchPath = "sf";
            mon_sf_connString = builder.ConnectionString;

            builder.Database = "pubmed";
            builder.SearchPath = "pp";
            pubmed_pp_connString = builder.ConnectionString;

            folder_base = settings["folder_base"];

            // example appsettings.json file...
            // the only values required are for...
            // {
            //    "host": "host_name...",
            //    "user": "user_name...",
            //    "password": "user_password...",
            //    "folder_base": "C:\\MDR JSON\\Object JSON... "
            // }
        }



        // Uses the _folder_base value from the settings file and
        // combines it with a date string to provide a parent directory
        // for the processed files.

        public string GetFolderBase()
        {
            //string today = DateTime.Now.ToString("yyyyMMdd");
            return folder_base;
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
            using (IDbConnection Conn = new NpgsqlConnection(pubmed_pp_connString))
            {
                Conn.Insert<ExtractionNote>(en);
            }
        }


        public bool FileInDatabase(string sd_oid)
        {
            string sql_string = "SELECT count(*) FROM sd.data_objects";
            sql_string += " where sd_oid = " + sd_oid;
            using (IDbConnection Conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                long res = (long)Conn.ExecuteScalar(sql_string);
                return (res > 0);
            }
        }


        // Inserts the base Data object, i.e. with all the  
        // singleton properties, in the database.

        public void StoreDataObject(CitationObject_in_DB cdb)
        {
            using (IDbConnection Conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                Conn.Insert<CitationObject_in_DB>(cdb);
            }
        }


        // Inserts the contributor (person or organisation) records for each Data.

        public ulong StoreContributors(PostgreSQLCopyHelper<ObjectContributor> copyHelper, IEnumerable<ObjectContributor> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of contributor identifier records (usually ORCIDs) for each Data.

        public ulong StoreContribIdentifiers(PostgreSQLCopyHelper<PersonIdentifier> copyHelper, IEnumerable<PersonIdentifier> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of contributor affiliation records for each Data.

        public ulong StoreContribAffiliations(PostgreSQLCopyHelper<PersonAffiliation> copyHelper, IEnumerable<PersonAffiliation> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of identifiers records associated with each Data.

        public ulong StoreObjectIdentifiers(PostgreSQLCopyHelper<ObjectIdentifier> copyHelper, IEnumerable<ObjectIdentifier> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of date records associated with each Data.

        public ulong StoreObjectDates(PostgreSQLCopyHelper<ObjectDate> copyHelper, IEnumerable<ObjectDate> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of language records associated with each Data.

        public ulong StoreObjectLanguages(PostgreSQLCopyHelper<ObjectLanguage> copyHelper, IEnumerable<ObjectLanguage> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of description records associated with each Data.

        public ulong StoreDescriptions(PostgreSQLCopyHelper<ObjectDescription> copyHelper, IEnumerable<ObjectDescription> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of instance records associated with each Data.

        public ulong StoreInstances(PostgreSQLCopyHelper<ObjectInstance> copyHelper, IEnumerable<ObjectInstance> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of 'databank' accession records associated with each Data,
        // including any linked ClinicalTrials.gov NCT numbers.

        public ulong StoreAcessionNumbers(PostgreSQLCopyHelper<ObjectDBAccessionNumber> copyHelper, IEnumerable<ObjectDBAccessionNumber> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of publication type records associated with each Data.

        public ulong StorePublicationTypes(PostgreSQLCopyHelper<ObjectPublicationType> copyHelper, IEnumerable<ObjectPublicationType> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of title records associated with each Data.

        public ulong StoreObjectTitles(PostgreSQLCopyHelper<ObjectTitle> copyHelper, IEnumerable<ObjectTitle> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of any comments records associated with each Data.

        public ulong StoreComments(PostgreSQLCopyHelper<ObjectCommentCorrection> copyHelper, IEnumerable<ObjectCommentCorrection> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Should inserts the set of keyword records associated with each Data.
        // Not used at the moment - see below.

        public ulong StoreObjectTopics(PostgreSQLCopyHelper<ObjectTopic> copyHelper, IEnumerable<ObjectTopic> entities)
        {
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }
                

        // Because the copyhelper mechanism does not work with the topic (not clear why), 
        // topic records are inserted indidually using this procedure.

        public void StoreTopic(ObjectTopic t)
        { 
            using (var conn = new NpgsqlConnection(pubmed_sd_connString))
            {
                conn.Insert<ObjectTopic>(t);
                
            }
        }


    }


}
