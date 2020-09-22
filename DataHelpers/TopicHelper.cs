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


        // indicates if the relevant topics table has any mesh coded values

        public bool topics_have_codes(string source_type)
        {
            string sql_string = @"select count(*) from ";
            sql_string += source_type.ToLower() == "study"
                                ? "sd.study_topics "
                                : "sd.object_topics ";
            sql_string += @" where topic_code is not null";
            
            int res = 0;
            using (var conn = new NpgsqlConnection(db_conn))
            {
                res = conn.ExecuteScalar<int>(sql_string);
            }
            return (res > 0);
        }


        // take ditinct mesh code / value pairs
        public void add_mesh_codes(string source_type)
        {
            string sql_string = @"Insert into context_ctx.topics_in_mesh(mesh_code, topic_value)
                   select distinct t.topic_code, t.topic_value from ";
            sql_string += source_type.ToLower() == "study"
                                ? "sd.study_topics t"
                                : "sd.object_topics t";
            sql_string += @" left join context_ctx.topics_in_mesh m
                             on t.topic_code = m.mesh_code
                             where t.topic_code is not null
                             and m.mesh_code is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

        }


        public void update_topics(string source_type)
        {
            string sql_string = @"Update ";
            sql_string += source_type.ToLower() == "study"
                                ? "sd.study_topics t"
                                : "sd.object_topics t";
            sql_string += @" set topic_code = m.mesh_code
                             from context_ctx.topics_in_mesh m
                             where t.topic_code is null
                             and t.topic_value = m.topic_value";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void store_unmatched_topic_values(string source_type, int source_id)
        {
            string sql_string = @"delete from context_ctx.topics_to_match where source_id = "
            + source_id.ToString() + @";
            insert into context_ctx.topics_to_match (source_id, topic_value, number_of) 
            select " + source_id.ToString() + @", topic_value, count(topic_value)";

            sql_string += source_type.ToLower() == "study"
                                ? " from sd.study_topics t"
                                : " from sd.object_topics t";
            sql_string += @" where t.topic_code is null 
                             group by t.topic_value;";
            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

        }
    }
}
