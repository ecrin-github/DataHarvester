using Dapper;
using Npgsql;

namespace DataHarvester
{
    public class PubmedPostProcBuilder
    {
        private string connString;
        private Source source;
        private PubmedHelper pub_helper;


        public PubmedPostProcBuilder(string _connString, Source _source)
        {
            connString = _connString;
            source = _source;
            pub_helper = new PubmedHelper(connString);
        }


        public void EstablishContextForeignTables(string user_name, string password)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                     schema sd;";
                conn.Execute(sql_string);

                sql_string = @"CREATE SERVER IF NOT EXISTS context "
                           + @" FOREIGN DATA WRAPPER postgres_fdw
                             OPTIONS (host 'localhost', dbname 'context', port '5432');";
                conn.Execute(sql_string);

                sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER context 
                     OPTIONS (user '" + user_name + "', password '" + password + "');";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_ctx cascade;
                     CREATE SCHEMA context_ctx; 
                     IMPORT FOREIGN SCHEMA ctx
                     FROM SERVER context 
                     INTO context_ctx;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_lup cascade;
                     CREATE SCHEMA context_lup; 
                     IMPORT FOREIGN SCHEMA lup
                     FROM SERVER context 
                     INTO context_lup;";
                conn.Execute(sql_string);
            }
        }


        public void ObtainPublisherNames()
        {
            pub_helper.obtain_publisher_names_using_eissn();
            pub_helper.obtain_publisher_names_using_pissn();
            pub_helper.obtain_publisher_names_using_journal_names();
        }

        public void UpdatePublisherOrgIds()
        {
            pub_helper.update_publisher_ids_using_default_names();
            pub_helper.update_publisher_ids_using_other_names();
            pub_helper.update_publisher_names_to_defaults();
        }

        public void UpdateIdentifierPublisherData()
        {
            pub_helper.update_identifiers_publishers_ids();
            pub_helper.update_identifiers_publishers_names();
        }

        public void CreateDataObjectsTable()
        {
            pub_helper.store_unmatched_publisher_org_names(source.id);
            pub_helper.transfer_citation_objects_to_data_objects();
        }

        public void CreateTotalLinksTable()
        {
            pub_helper.store_bank_links_in_pp_schema();
            pub_helper.combine_distinct_study_pubmed_links();
        }


        public void DropContextForeignTables()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER context;";
                conn.Execute(sql_string);

                sql_string = @"DROP SERVER IF EXISTS context CASCADE;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_ctx;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_lup;";
                conn.Execute(sql_string);
            }
        }
    }
}

