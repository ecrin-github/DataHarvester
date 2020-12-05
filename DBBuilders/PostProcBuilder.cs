using Dapper;
using Npgsql;

namespace DataHarvester
{
    public class PostProcBuilder
    {
        private string connString;
        private Source source;
        private OrgHelper org_helper;
        private TopicHelper topic_helper;

        public PostProcBuilder(string _connString, Source _source, LoggingDataLayer _logging_repo)
        {
            connString = _connString;
            source = _source;
            org_helper = new OrgHelper(connString, _logging_repo);
            topic_helper = new TopicHelper(connString, _logging_repo );
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

        public void UpdateStudyIdentifierOrgs()
        {
            org_helper.update_study_identifiers_using_default_name();
            org_helper.update_study_identifiers_using_other_name();
            org_helper.update_study_identifiers_insert_default_names();
        }


        public void UpdateStudyContributorOrgs()
        {
            if (source.has_study_contributors)
            {
                org_helper.update_study_contributors_using_default_name();
                org_helper.update_study_contributors_using_other_name();
                org_helper.update_study_contributors_insert_default_names();
                org_helper.update_missing_sponsor_ids();
            }
        }

        public void UpdateDataObjectOrgs()
        {
            org_helper.update_data_objects_using_default_name();
            org_helper.update_data_objects_using_other_name();
            org_helper.update_data_objects_insert_default_names();
        }

        public void StoreUnMatchedNames()
        {
            if (source.has_study_tables)
            {
                org_helper.store_unmatched_study_identifiers_org_names(source.id);
            }
            if (source.has_study_tables && source.has_study_contributors)
            {
                org_helper.store_unmatched_study_contributors_org_names(source.id);
            }
            org_helper.store_unmatched_data_object_org_names(source.id);
        }


        public void UpdateTopics(string source_type)
        {
            if ((source_type == "study" && source.has_study_topics)
                 || source_type == "object")
            {
                topic_helper.delete_humans_as_topic(source_type);
                topic_helper.update_geographic_topics(source_type);
                topic_helper.update_topics(source_type);
                topic_helper.store_unmatched_topic_values(source_type, source.id);
            }
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

