using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataHarvester
{
    public class LoggingDataLayer
    {
        private string connString;
        private string context_connString;
        private Source source;
        private string sql_file_select_string;
        private string logfile_startofpath;
        private string logfile_path;
        private string host;
        private string user;
        private string password;
        private StreamWriter sw;

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
            host = settings["host"];
            user = settings["user"];
            password = settings["password"];

            builder.Host = host;
            builder.Username = user;
            builder.Password = password;

            builder.Database = "mon";
            connString = builder.ConnectionString;

            builder.Database = "context";
            context_connString = builder.ConnectionString;

            logfile_startofpath = settings["logfilepath"];

            sql_file_select_string = "select id, source_id, sd_id, remote_url, last_revised, ";
            sql_file_select_string += " assume_complete, download_status, local_path, last_saf_id, last_downloaded, ";
            sql_file_select_string += " last_harvest_id, last_harvested, last_import_id, last_imported ";

        }

        public Source SourceParameters => source;

        public void OpenLogFile(string database_name)
        {
            string dt_string = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
                              .Replace("-", "").Replace(":", "").Replace("T", " ");
            logfile_path = logfile_startofpath + "HV " + database_name + " " + dt_string + ".log";
            sw = new StreamWriter(logfile_path, true, System.Text.Encoding.UTF8);
        }

        public void LogLine(string message, string identifier = "")
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string feedback = dt_string + message + identifier;
            Transmit(feedback);
        }

        public void LogHeader(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string header = dt_string + "**** " + message + " ****";
            Transmit("");
            Transmit(header);
        }

        public void LogError(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string error_message = dt_string + "***ERROR*** " + message;
            Transmit("");
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit(error_message);
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit("");
        }

        public void CloseLog()
        {
            LogHeader("Closing Log");
            sw.Flush();
            sw.Close();
        }

        private void Transmit(string message)
        {
            sw.WriteLine(message);
            Console.WriteLine(message);
        }

        public void LogParameters(Source source, int harvest_type_id, bool? org_update_only)
        {
            OpenLogFile(source.database_name);
            LogLine("****** HARVEST ******");
            LogHeader("Setup");
            LogLine("Source_id is " + source.id.ToString());
            LogLine("Type_id is " + harvest_type_id.ToString());
            string org_update = (org_update_only == null) ? " was not provided" : " is " + org_update_only;
            LogLine("Update org ids only" + org_update);
        }


        public void LogTableStatistics(Source s)
        {
            // Gets and logs record count for each table in the sd schema of the database
            // Start by obtaining conection string, then construct log line for each by 
            // calling db interrogation for each applicable table
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = host;
            builder.Username = user;
            builder.Password =password;
            builder.Database = source.database_name;
            string db_conn = builder.ConnectionString;

            LogHeader("Results");
            if (source.has_study_tables)
            {
                LogLine(GetTableRecordCount(db_conn, "studies"));
                LogLine(GetTableRecordCount(db_conn, "study_identifiers"));
                LogLine(GetTableRecordCount(db_conn, "study_titles"));

                // these are database dependent
                if (source.has_study_topics) LogLine(GetTableRecordCount(db_conn, "study_topics")); 
                if (source.has_study_features) LogLine(GetTableRecordCount(db_conn, "study_features")); 
                if (source.has_study_contributors) LogLine(GetTableRecordCount(db_conn, "study_contributors"));
                if (source.has_study_references) LogLine(GetTableRecordCount(db_conn, "study_references")); 
                if (source.has_study_relationships) LogLine(GetTableRecordCount(db_conn, "study_relationships")); 
                if (source.has_study_links) LogLine(GetTableRecordCount(db_conn, "study_links")); 
                if (source.has_study_ipd_available) LogLine(GetTableRecordCount(db_conn, "study_ipd_available")); 
            }

            LogLine(GetTableRecordCount(db_conn, "study_hashes"));
            IEnumerable<hash_stat> study_hash_stats = (GetHashStats(db_conn, "study_hashes"));
            if (study_hash_stats.Count() > 0)
            {
                LogLine("from the hashes");
                foreach (hash_stat hs in study_hash_stats)
                {
                    LogLine(hs.num.ToString() + " study records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
                }
            }

            // these common to all databases
            LogLine("");
            LogLine(GetTableRecordCount(db_conn, "data_objects"));
            LogLine(GetTableRecordCount(db_conn, "object_instances"));
            LogLine(GetTableRecordCount(db_conn, "object_titles"));

            // these are database dependent		

            if (source.has_object_datasets) LogLine(GetTableRecordCount(db_conn, "object_datasets")); 
            if (source.has_object_dates) LogLine(GetTableRecordCount(db_conn, "object_dates"));
            if (source.has_object_relationships) LogLine(GetTableRecordCount(db_conn, "object_relationships")); 
            if (source.has_object_rights) LogLine(GetTableRecordCount(db_conn, "object_rights")); 
            if (source.has_object_pubmed_set)
            {
                LogLine(GetTableRecordCount(db_conn, "citation_objects"));
                LogLine(GetTableRecordCount(db_conn, "object_contributors")); 
                LogLine(GetTableRecordCount(db_conn, "study_topics"));
                LogLine(GetTableRecordCount(db_conn, "object_comments")); 
                LogLine(GetTableRecordCount(db_conn, "object_descriptions"));
                LogLine(GetTableRecordCount(db_conn, "object_identifiers")); 
                LogLine(GetTableRecordCount(db_conn, "object_db_links")); 
                LogLine(GetTableRecordCount(db_conn, "object_publication_types")); 
            }

            LogLine(GetTableRecordCount(db_conn, "object_hashes"));
            IEnumerable<hash_stat> object_hash_stats = (GetHashStats(db_conn, "object_hashes"));
            if (object_hash_stats.Count() > 0)
            {
                LogLine("from the hashes");
                foreach (hash_stat hs in object_hash_stats)
                {
                    LogLine(hs.num.ToString() + " object records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
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



        public Source FetchSourceParameters(int source_id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                source = Conn.Get<Source>(source_id);
                return source;
            }
        }

        public int GetNextHarvestEventId()
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = "select max(id) from sf.harvest_events ";
                int last_id = Conn.ExecuteScalar<int>(sql_string);
                return last_id + 1;
            }

        }

        public IEnumerable<StudyFileRecord> FetchStudyFileRecords(int source_id, int harvest_type_id = 1)
        {
            string sql_string = sql_file_select_string;
            sql_string += " from sf.source_data_studies ";
            sql_string += GetWhereClause(source_id, harvest_type_id);
            sql_string += " order by local_path";

            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                return Conn.Query<StudyFileRecord>(sql_string);
            }
        }

        public IEnumerable<ObjectFileRecord> FetchObjectFileRecords(int source_id, int harvest_type_id = 1)
        {
            string sql_string = sql_file_select_string;
            sql_string += " from sf.source_data_objects";
            sql_string += GetWhereClause(source_id, harvest_type_id);
            sql_string += " order by local_path";

            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                return Conn.Query<ObjectFileRecord>(sql_string);
            }
        }


        public int FetchFileRecordsCount(int source_id, string source_type,
                                       int harvest_type_id = 1, DateTime? cutoff_date = null)
        {
            string sql_string = "select count(*) ";
            sql_string += source_type.ToLower() == "study" ? "from sf.source_data_studies"
                                                 : "from sf.source_data_objects";
            sql_string += GetWhereClause(source_id, harvest_type_id);

            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                return Conn.ExecuteScalar<int>(sql_string);
            }
        }


        public int FetchFullFileCount(int source_id, string source_type)
        {
            string sql_string = "select count(*) ";
            sql_string += source_type.ToLower() == "study" ? "from sf.source_data_studies"
                                                 : "from sf.source_data_objects";
            sql_string += " where source_id = " + source_id.ToString();
            sql_string += " and local_path is not null";

            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                return Conn.ExecuteScalar<int>(sql_string);
            }
        }


        public IEnumerable<StudyFileRecord> FetchStudyFileRecordsByOffset(int source_id, int offset_num,
                                      int amount, int harvest_type_id = 1)
        {
            string sql_string = sql_file_select_string;
            sql_string += " from sf.source_data_studies ";
            sql_string += GetWhereClause(source_id, harvest_type_id);
            sql_string += " order by local_path ";
            sql_string += " offset " + offset_num.ToString() + " limit " + amount.ToString();

            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                return Conn.Query<StudyFileRecord>(sql_string);
            }
        }

        public IEnumerable<ObjectFileRecord> FetchObjectFileRecordsByOffset(int source_id, int offset_num,
                                     int amount, int harvest_type_id = 1)
        {
            string sql_string = sql_file_select_string;
            sql_string += " from sf.source_data_objects ";
            sql_string += GetWhereClause(source_id, harvest_type_id);
            sql_string += " order by local_path ";
            sql_string += " offset " + offset_num.ToString() + " limit " + amount.ToString();

            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                return Conn.Query<ObjectFileRecord>(sql_string);
            }
        }

        private string GetWhereClause(int source_id, int harvest_type_id)
        {
            string where_clause = "";
            if (harvest_type_id == 1)
            {
                // Count all files.
                where_clause = " where source_id = " + source_id.ToString();
            }
            else if (harvest_type_id == 2)
            {
                // Harvest files that have been downloaded since the last import, 
                // NOTE - not since the last harvest, as multiple harvests may have
                // been carried out. A file should be harvested for import if it 
                // has not yet been imported, or a new download (possible a new version) 
                // has taken place since the import.
                // So files needed where their download date > import date, or they are new
                // and therefore have a null import date

                where_clause = " where source_id = " + source_id.ToString() +
                               " and (last_downloaded >= last_imported or last_imported is null) ";
            }
            where_clause += " and local_path is not null";
            
            return where_clause;
        }

        // get record of interest
        public StudyFileRecord FetchStudyFileRecord(string sd_id, int source_id, string source_type)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = sql_file_select_string;
                sql_string += " from sf.source_data_studies";
                sql_string += " where sd_id = '" + sd_id + "' and source_id = " + source_id.ToString();
                return Conn.Query<StudyFileRecord>(sql_string).FirstOrDefault();
            }
        }


        public ObjectFileRecord FetchObjectFileRecord(string sd_id, int source_id, string source_type)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = sql_file_select_string;
                sql_string += " from sf.source_data_objects";
                sql_string += " where sd_id = '" + sd_id + "' and source_id = " + source_id.ToString();
                return Conn.Query<ObjectFileRecord>(sql_string).FirstOrDefault();
            }
        }

        public void UpdateFileRecLastHarvested(int id, string source_type, int last_harvest_id)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = source_type.ToLower() == "study" ? "update sf.source_data_studies"
                                                           : "update sf.source_data_objects";
                sql_string += " set last_harvest_id = " + last_harvest_id.ToString() + ", ";
                sql_string += " last_harvested = current_timestamp";
                sql_string += " where id = " + id.ToString();
                conn.Execute(sql_string);
            }
        }

        public int StoreHarvestEvent(HarvestEvent harvest)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                return (int)conn.Insert<HarvestEvent>(harvest);
            }
        }


        // Stores an 'extraction note', e.g. an unusual occurence found and
        // logged during the extraction, in the associated table.

        public void StoreExtractionNote(ExtractionNote ext_note)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Insert<ExtractionNote>(ext_note);
            }
        }


        // gets a 2 letter language code rather than thean the original 3
        public string lang_3_to_2(string lang_code_3)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(context_connString))
            {
                string sql_string = "select code from lup.language_codes where ";
                sql_string += " marc_code = '" + lang_code_3 + "';";
                return Conn.Query<string>(sql_string).FirstOrDefault();
            }
        }


    }

}

