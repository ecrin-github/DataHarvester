using System;
using System.Collections.Generic;
using System.Text;
using Serilog;
using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;
using System.Linq;

namespace DataHarvester
{
    public class LoggerHelper
    {
        private ILogger _logger;

        public LoggerHelper(ILogger logger)
        {
            _logger = logger;
        }

        public void LogTableStatistics(Credentials credentials, Source s)
        {
            // Gets and logs record count for each table in the sd schema of the database
            // Start by obtaining conection string, then construct log line for each by 
            // calling db interrogation for each applicable table
            NpgsqlConnectionStringBuilder statsbuilder = new NpgsqlConnectionStringBuilder();
            statsbuilder.Host = credentials.Host;
            statsbuilder.Username = credentials.Username;
            statsbuilder.Password = credentials.Password;
            statsbuilder.Database = s.database_name;
            string db_conn = statsbuilder.ConnectionString;

            _logger.Information("Results\n");
            if (s.has_study_tables)
            {
                _logger.Information(GetTableRecordCount(db_conn, "studies"));
                _logger.Information(GetTableRecordCount(db_conn, "study_identifiers"));
                _logger.Information(GetTableRecordCount(db_conn, "study_titles"));

                // these are database dependent
                if (s.has_study_topics) _logger.Information(GetTableRecordCount(db_conn, "study_topics"));
                if (s.has_study_features) _logger.Information(GetTableRecordCount(db_conn, "study_features"));
                if (s.has_study_contributors) _logger.Information(GetTableRecordCount(db_conn, "study_contributors"));
                if (s.has_study_references) _logger.Information(GetTableRecordCount(db_conn, "study_references"));
                if (s.has_study_relationships) _logger.Information(GetTableRecordCount(db_conn, "study_relationships"));
                if (s.has_study_links) _logger.Information(GetTableRecordCount(db_conn, "study_links"));
                if (s.has_study_ipd_available) _logger.Information(GetTableRecordCount(db_conn, "study_ipd_available"));

                _logger.Information(GetTableRecordCount(db_conn, "study_hashes"));
                IEnumerable<hash_stat> study_hash_stats = (GetHashStats(db_conn, "study_hashes"));
                if (study_hash_stats.Count() > 0)
                {
                    _logger.Information("from the hashes");
                    foreach (hash_stat hs in study_hash_stats)
                    {
                        _logger.Information(hs.num.ToString() + " study records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
                    }
                }
            }

            // these common to all databases
            _logger.Information("");
            _logger.Information(GetTableRecordCount(db_conn, "data_objects"));
            _logger.Information(GetTableRecordCount(db_conn, "object_instances"));
            _logger.Information(GetTableRecordCount(db_conn, "object_titles"));

            // these are database dependent		

            if (s.has_object_datasets) _logger.Information(GetTableRecordCount(db_conn, "object_datasets"));
            if (s.has_object_dates) _logger.Information(GetTableRecordCount(db_conn, "object_dates"));
            if (s.has_object_relationships) _logger.Information(GetTableRecordCount(db_conn, "object_relationships"));
            if (s.has_object_rights) _logger.Information(GetTableRecordCount(db_conn, "object_rights"));
            if (s.has_object_pubmed_set)
            {
                _logger.Information(GetTableRecordCount(db_conn, "citation_objects"));
                _logger.Information(GetTableRecordCount(db_conn, "object_contributors"));
                _logger.Information(GetTableRecordCount(db_conn, "object_topics"));
                _logger.Information(GetTableRecordCount(db_conn, "object_comments"));
                _logger.Information(GetTableRecordCount(db_conn, "object_descriptions"));
                _logger.Information(GetTableRecordCount(db_conn, "object_identifiers"));
                _logger.Information(GetTableRecordCount(db_conn, "object_db_links"));
                _logger.Information(GetTableRecordCount(db_conn, "object_publication_types"));
            }

            _logger.Information(GetTableRecordCount(db_conn, "object_hashes"));
            IEnumerable<hash_stat> object_hash_stats = (GetHashStats(db_conn, "object_hashes"));
            if (object_hash_stats.Count() > 0)
            {
                _logger.Information("from the hashes");
                foreach (hash_stat hs in object_hash_stats)
                {
                    _logger.Information(hs.num.ToString() + " object records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
                }
            }
        }


        private string GetTableRecordCount(string db_conn, string table_name)
        {
            string sql_string = "select count(*) from sd." + table_name;

            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                int res = conn.ExecuteScalar<int>(sql_string);
                return res.ToString() + " records found in sd." + table_name;
            }
        }


        private IEnumerable<hash_stat> GetHashStats(string db_conn, string table_name)
        {
            string sql_string = "select hash_type_id, hash_type, count(id) as num from sd." + table_name;
            sql_string += " group by hash_type_id, hash_type order by hash_type_id;";

            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                return conn.Query<hash_stat>(sql_string);
            }
        }


    }
}
