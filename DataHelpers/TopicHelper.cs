using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
    public class TopicHelper
    {
        string db_conn;

        public TopicHelper(string _db_conn)
        {
            db_conn = _db_conn;
        }


        // delete humans as subjects - as clinical research on humans...
    
        public void delete_humans_as_topic(string source_type)
        {
            string sql_string = @"delete from ";
            sql_string += source_type.ToLower() == "study"
                                ? "sd.study_topics "
                                : "sd.object_topics ";
            sql_string += @" where topic_value = 'Human' 
                             or topic_value = 'Humans';";
           

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // identify any geographic terms amongst the topics

        public void update_geographic_topics(string source_type)
        {
            string sql_string = source_type.ToLower() == "study" 
                                ? "update sd.study_topics t "
                                : "update sd.object_topics t ";
            sql_string += @"set topic_type_id = 16,
                                  topic_type = 'geographic'
                                  from context_ctx.geog_entities g
                                  where t.topic_value = g.name
                                  and topic_type is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // indicates if the relevant topics table has any coded values

         public bool topics_have_codes(string source_type)
        {
            string sql_string = @"select count(*) from ";
            sql_string += source_type.ToLower() == "study"
                                ? "sd.study_topics "
                                : "sd.object_topics ";
            sql_string += @" where topic_ct_code is not null";
            int res = 0;
            using (var conn = new NpgsqlConnection(db_conn))
            {
                res = conn.ExecuteScalar<int>(sql_string);
            }
            return (res > 0);
        }        
        
        
        public void update_topic_types(string source_type)
        {
            string sql_string = @"with t as (
                            select id as type_id, name as topic_name
                            from context_lup.topic_types
                        )
                        ";
            sql_string += source_type.ToLower() == "study"
                                    ? " update sd.study_topics p "
                                    : " update sd.object_topics p ";
            sql_string += @"set topic_type_id = t.type_id
                        from t
                        where lower(p.topic_type) = lower(t.topic_name)
                        and topic_type_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void setup_temp_topic_tables(string source_type)
        {
            string sql_string = "";
            sql_string = @"DROP TABLE IF EXISTS sd.temp_topics_with_code; 
                           CREATE TABLE sd.temp_topics_with_code
                           as select * from ";
            sql_string += source_type.ToLower() == "study"
                                    ? " sd.study_topics "
                                    : " sd.object_topics ";
            sql_string += " where topic_ct_code is not null;";


            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

            sql_string = @"DROP TABLE IF EXISTS sd.temp_topics_without_code;
                           CREATE TABLE sd.temp_topics_without_code
                           as select * from ";
            sql_string += source_type.ToLower() == "study"
                                    ? " sd.study_topics "
                                    : " sd.object_topics ";
            sql_string += " where topic_ct_code is null;";


            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

        }

        public void de_duplicate_topics(string source_type)
        {
            // delete topics in temp_topics_without_code
            // that are duplicates of those in temp_topics_with_code

            string sql_string = "";
            sql_string = @"delete from sd.temp_topics_without_code w
                           using sd.temp_topics_with_code c ";
            sql_string += source_type.ToLower() == "study"
                                    ? " where w.sd_sid = c.sd_sid "
                                    : " where w.sd_oid = c.sd_oid ";
            sql_string += " and lower(w.topic_value) = lower(c.topic_value);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }


            // Create a list of distinct code terms linked to topic names

            sql_string = @"DROP TABLE IF EXISTS sd.temp_code_list;
                           CREATE TABLE sd.temp_code_list
                           as 
                           select distinct topic_value, topic_ct_code ";
            sql_string += source_type.ToLower() == "study"
                                    ? " from sd.study_topics "
                                    : " from sd.object_topics ";
            sql_string += " where topic_ct_code is not null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }


            // use the look up table to add mesh codes where possible

            sql_string = @"UPDATE sd.temp_topics_without_code w
                           set topic_ct_code = m.topic_ct_code
                           from sd.temp_code_list m
                           where lower(w.topic_value) = lower(m.topic_value);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }


            sql_string = @"DROP TABLE sd.temp_code_list;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void reassemble_topic_tables(string source_type)
        {
            string sql_string = "";

            // put the two tables back together again
                
            sql_string = @"create table sd.new_topics as 
                           select * from sd.temp_topics_with_code
                           union
                           select * from sd.temp_topics_without_code;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }


            // rename the tables 
            // study_topics to old_topics
            // new topics to study_topics

            sql_string = @"DROP TABLE IF EXISTS sd.old_topics";
            sql_string += source_type.ToLower() == "study"
                                    ? @" ALTER TABLE sd.study_topics RENAME TO old_topics;
                                         ALTER TABLE sd.new_topics RENAME TO study_topics; "
                                    : @" ALTER TABLE sd.object_topics RENAME TO old_topics;
                                         ALTER TABLE sd.new_topics RENAME TO object_topics; ";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

            sql_string = @"DROP TABLE sd.temp_topics_with_code;
                           DROP TABLE sd.temp_topics_without_code;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

        }
    }
}
