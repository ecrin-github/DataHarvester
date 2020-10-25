using Dapper;
using Npgsql;
using System;

namespace DataHarvester
{
    public class TopicHelper
    {
        string db_conn;

        public TopicHelper(string _db_conn)
        {
            db_conn = _db_conn;
        }

        public void ExecuteSQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        // delete humans as subjects - as clinical research on humans...

        public void delete_humans_as_topic(string source_type)
        {
            string sql_string = @"delete from ";
            sql_string += source_type.ToLower() == "study"
                                ? "sd.study_topics "
                                : "sd.object_topics ";
            sql_string += @" where lower(topic_value) = 'human' 
                             or lower(topic_value) = 'humans'
                             or lower(topic_value) = 'other'
                             or lower(topic_value) = 'studies'
                             or lower(topic_value) = 'evaluation';";
            ExecuteSQL(sql_string);
            StringHelpers.SendFeedback("Updating topic codes, deleting meanngless categories");
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
            ExecuteSQL(sql_string);
            StringHelpers.SendFeedback("Updating topic codes - labelling geographic entities");
        }


        public void update_topics(string source_type)
        {
            // can be difficult to do ths with large datasets
            int rec_count = 0;
            int rec_batch = 500000;
            string sql_string = @"select count(*) from sd.";
            sql_string += source_type.ToLower() == "study"
                                ? "study_topics;"
                                : "object_topics;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                rec_count = conn.ExecuteScalar<int>(sql_string);
            }

            // in some cases mesh codes may be overwritten if 
            // they do not conform entirely (in format) with the mesh list

            sql_string = @"Update ";
            sql_string += source_type.ToLower() == "study"
                                ? "sd.study_topics t "
                                : "sd.object_topics t ";
            sql_string += @" set topic_code = m.code,
                             topic_value = m.term,
                             mesh_coded = true
                             from context_ctx.mesh_lookup m
                             where lower(t.topic_value) = m.entry";
            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Updating topic codes, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    StringHelpers.SendFeedback("Updating topic codes - as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In update_topics: " + res);
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
            ExecuteSQL(sql_string); 
        }
    }
}
