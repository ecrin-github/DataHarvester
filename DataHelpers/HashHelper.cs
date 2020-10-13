using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
    public class HashHelper
    {
        string db_conn;

        public HashHelper(string _db_conn)
        {
            db_conn = _db_conn;
        }

        public int GetRecordCount(string table_name)
        {
            int res = 0;
            string sql_string = @"select count(*) from sd." + table_name;
            using (var conn = new NpgsqlConnection(db_conn))
            {
                res = conn.ExecuteScalar<int>(sql_string);
            }
            return res;
        }

        public int GetHashRecordCount(string table_name, int hash_type_id)
        {
            int res = 0;
            string sql_string = @"select count(*) from sd." + table_name + 
                " where hash_type_id = " + hash_type_id.ToString();
            using (var conn = new NpgsqlConnection(db_conn))
            {
                res = conn.ExecuteScalar<int>(sql_string);
            }
            return res;
        }


        public void ExecuteSQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void ExecuteHashSQL(string sql_string, string table_name)
        {
            try
            {
                int rec_count = GetRecordCount(table_name);
                int rec_batch = 500000;
                // int rec_batch = 10000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " where id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Creating " + table_name + " hash codes, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    StringHelpers.SendFeedback("Creating " + table_name + " hash codes - as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In hash creation (" + table_name + "): " + res);
            }
        }


        public void CreateCompositeOjectHashes(string top_sql_string, int hash_type_id, string hash_type)
        {
            try
            {
                int rec_count = GetRecordCount("data_objects");
                int hash_count = GetHashRecordCount("object_hashes", hash_type_id);
                int num_recs_per_entity = hash_count / rec_count;
                bool use_batch = false;
                int rec_batch = 0;
                if (rec_count > 50000)
                {
                    use_batch = true;
                    rec_batch = 50000;
                    if (num_recs_per_entity > 2)
                    {
                        rec_batch = 50000 / (2 * num_recs_per_entity);
                    }
                }
                
                if (use_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string where_sql_string = " where d.id >= " + r.ToString() + " and d.id < " + (r + rec_batch).ToString();

                        string batch_sql_string = top_sql_string + @" t 
                                 inner join sd.data_objects d 
                                 on d.sd_oid = t.sd_oid 
                                 " + where_sql_string + " group by t.sd_oid;";
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Creating composite object hash codes (" + hash_type + "), " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    string sql_string = top_sql_string + @" t group by t.sd_oid;";
                    ExecuteSQL(sql_string);
                    StringHelpers.SendFeedback("Creating composite object hash codes (" + hash_type + ") as a single batch");
                }

            }
            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In object composite hash creation (" + hash_type + "): " + res);
            }
        }

        public void CreateFullDataObjectHashes(string top_sql_string)
        {
            try
            {
                int rec_count = GetRecordCount("data_objects");
                int rec_batch = 50000;
                // int rec_batch = 1000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string where_sql_string = " and d.id >= " + r.ToString() + " and d.id < " + (r + rec_batch).ToString();
                        string batch_sql_string = top_sql_string + where_sql_string;
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Creating full object hash codes, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(top_sql_string);
                    StringHelpers.SendFeedback("Creating full object hash codes - as a single batch");
                }

            }
            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In full hash creation for data objects: " + res);
            }
        }


        public void CreateCompositeStudyHashes(string top_sql_string, int hash_type_id, string hash_type)
        {
            try
            {
                int rec_count = GetRecordCount("studies");
                int rec_batch = 10000;
                //int rec_batch = 1000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string where_sql_string = " where s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();

                        string batch_sql_string = top_sql_string + @" t 
                                 inner join sd.studies s 
                                 on s.sd_sid = t.sd_sid 
                                 " + where_sql_string + " group by t.sd_sid;";
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Creating composite study hash codes (" + hash_type + "), " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    string sql_string = top_sql_string + @" t group by t.sd_sid;";
                    ExecuteSQL(sql_string);
                    StringHelpers.SendFeedback("Creating composite study hash codes (" + hash_type + ") as a single batch");
                }

            }
            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In study composite hash creation: " + res);
            }
        }

        public void CreateFullStudyHashes(string top_sql_string)
        {
            try
            {
                int rec_count = GetRecordCount("studies");
                int rec_batch = 50000;
                //int rec_batch = 1000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string where_sql_string = " and s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                        string batch_sql_string = top_sql_string + where_sql_string;
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Creating full study hash codes, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(top_sql_string);
                    StringHelpers.SendFeedback("Creating full study hash codes - as a single batch");
                }

            }
            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In full hash creation for studies: " + res);
            }
        }

    }


        public class StudyHashCreators
     {
        string db_conn;
        HashHelper h;

        public StudyHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
            h = new HashHelper(db_conn);
        }

        public void create_study_record_hashes()
        {
            string sql_string = @"Update sd.studies
              set record_hash = md5(json_build_array(display_title, brief_description, bd_contains_html,
              data_sharing_statement, dss_contains_html, study_start_year, study_start_month, study_type_id,
              study_status_id, study_enrolment, study_gender_elig_id, min_age,
              min_age_units_id, max_age, max_age_units_id)::varchar)";

            h.ExecuteHashSQL(sql_string, "studies");
        }


        public void create_study_identifier_hashes()
        {
            string sql_string = @"Update sd.study_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, identifier_org_id,
              identifier_org, identifier_date, identifier_link)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_identifiers");
        }


        public void create_study_title_hashes()
        {
            string sql_string = @"Update sd.study_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, lang_code,
              lang_usage_id, is_default, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_titles");
        }


        public void create_study_contributor_hashes()
        {
            string sql_string = @"Update sd.study_contributors
              set record_hash = md5(json_build_array(contrib_type_id, is_individual, organisation_id,
              organisation_name, person_id, person_given_name, person_family_name, person_full_name,
              person_identifier, identifier_type, person_affiliation, 
              affil_org_id, affil_org_id_type)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_contributors");
        }


        public void create_study_topic_hashes()
        {
            string sql_string = @"Update sd.study_topics
              set record_hash = md5(json_build_array(topic_type_id, topic_value, 
              mesh_coded, topic_code, topic_value, topic_qualcode,
		      topic_qualvalue, original_ct_id, original_ct_code,
		      original_value, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_topics");
        }


        public void create_study_feature_hashes()
        {
            string sql_string = @"Update sd.study_features
              set record_hash = md5(json_build_array(feature_type_id, feature_value_id)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_features");
        }


        public void create_study_reference_hashes()
        {
            string sql_string = @"Update sd.study_references
              set record_hash = md5(json_build_array(pmid, citation, doi, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_references");
        }


        public void create_study_relationship_hashes()
        {
            string sql_string = @"Update sd.study_relationships
              set record_hash = md5(json_build_array(relationship_type_id, target_sd_sid)::varchar)";
           
            h.ExecuteHashSQL(sql_string, "study_relationships");
        }


        public void create_study_link_hashes()
        {
            string sql_string = @"Update sd.study_links
              set record_hash = md5(json_build_array(link_label, link_url)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_links");
        }


        public void create_ipd_available_hashes()
        {
            string sql_string = @"Update sd.study_ipd_available
              set record_hash = md5(json_build_array(ipd_id, ipd_type, ipd_url, ipd_comment)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_ipd_available");
        }

    }


  
    public class StudyCompositeHashCreators
    {
        string db_conn;
        HashHelper h;

        public StudyCompositeHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
            h = new HashHelper(db_conn);
        }

        public void create_composite_study_hashes(int hash_type_id, string hash_type, string table_name)
        {
            string top_sql_string = @"Insert into sd.study_hashes 
                    (sd_sid, hash_type_id, hash_type, composite_hash)
                    select t.sd_sid, " + hash_type_id.ToString() + ", '" + hash_type + @"',  
                    md5(to_json(array_agg(t.record_hash ORDER BY t.record_hash))::varchar)
                    from sd." + table_name;

            h.CreateCompositeStudyHashes(top_sql_string, hash_type_id, hash_type);

        }

        public void create_full_study_hashes()
        {
            string sql_string = @"update sd.studies s
            set study_full_hash = b.rollup
            from (select sd_sid, md5(to_json(array_agg(hash ORDER BY hash))::varchar) as rollup
                  from 
                     (select sd_sid, composite_hash as hash 
                      from sd.study_hashes
                      union
                      select sd_sid, record_hash as hash
                      from sd.studies) h
	              group by sd_sid) b
            where 
            s.sd_sid = b.sd_sid";

            h.CreateFullStudyHashes(sql_string);
        }
    }


    public class ObjectHashCreators
    {
        string db_conn;
        HashHelper h;

        public ObjectHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
            h = new HashHelper(db_conn);
        }


        public void create_object_record_hashes()
        {
            string sql_string = @"Update sd.data_objects
              set record_hash = md5(json_build_array(display_title, version, doi, doi_status_id, publication_year,
              object_class_id, object_type_id, managing_org_id, managing_org, lang_code, access_type_id,
              access_details, access_details_url, eosc_category, add_study_contribs,
              add_study_topics)::varchar)";

            h.ExecuteHashSQL(sql_string, "data_objects");
        }


        public void create_recordset_properties_hashes()
        {
            string sql_string = @"Update sd.object_datasets
              set record_hash = md5(json_build_array(record_keys_type_id, record_keys_details,
              deident_type_id, deident_direct, deident_hipaa,
              deident_dates, deident_nonarr, deident_kanon, deident_details,
              consent_type_id, consent_noncommercial, consent_geog_restrict,
              consent_research_type, consent_genetic_only, consent_no_methods, consent_details)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_datasets");
        }


        public void create_object_date_hashes()
        {
            string sql_string = @"Update sd.object_dates
              set record_hash = md5(json_build_array(date_type_id, is_date_range, date_as_string, 
              start_year, start_month, start_day, end_year, end_month, end_day,
              details)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_dates");
        }


        public void create_object_instance_hashes()
        {
            string sql_string = @"Update sd.object_instances
              set record_hash = md5(json_build_array(instance_type_id, repository_org_id, 
              repository_org, url, url_accessible,  
              resource_type_id, resource_size, resource_size_units, resource_comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_instances");
        }


        public void create_object_title_hashes()
        {
            string sql_string = @"Update sd.object_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, lang_code, 
              lang_usage_id, is_default, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_titles");

        }


        public void create_object_contributor_hashes()
        {
            string sql_string = @"Update sd.object_contributors
              set record_hash = md5(json_build_array(contrib_type_id, is_individual, organisation_id,
              organisation_name, person_id, person_given_name, person_family_name, person_full_name,
              person_identifier, identifier_type, person_affiliation, 
              affil_org_id, affil_org_id_type)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_contributors");
        }


        public void create_object_topic_hashes()
        {
            string sql_string = @"Update sd.object_topics
              set record_hash = md5(json_build_array(topic_type_id, topic_value,
              mesh_coded, topic_code, topic_value, topic_qualcode,
              topic_qualvalue, original_ct_id, original_ct_code,
              original_value, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_topics");
        }


        public void create_object_comment_hashes()
        {
            string sql_string = @"Update sd.object_comments
              set record_hash = md5(json_build_array(ref_type, ref_source, pmid, pmid_version,
              notes)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_comments"); 
        }


        public void create_object_description_hashes()
        {
            string sql_string = @"Update sd.object_descriptions
              set record_hash = md5(json_build_array(description_type_id, label, description_text, lang_code,
              contains_html)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_descriptions");
        }


        public void create_object_identifier_hashes()
        {
            string sql_string = @"Update sd.object_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, 
                                identifier_org_id, identifier_org, identifier_date)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_identifiers");
        }


        public void create_object_db_link_hashes()
        {
            string sql_string = @"Update sd.object_db_links
              set record_hash = md5(json_build_array(db_sequence, db_name, 
              id_in_db)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_db_links");
        }


        public void create_object_publication_type_hashes()
        {
            string sql_string = @"Update sd.object_publication_types
              set record_hash = md5(json_build_array(type_name)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_publication_types");
        }


        public void create_object_relationship_hashes()
        {
            string sql_string = @"Update sd.object_relationships
              set record_hash = md5(json_build_array(relationship_type_id,
                                target_sd_oid)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_relationships");
        }


        public void create_object_right_hashes()
        {
            string sql_string = @"Update sd.object_rights
              set record_hash = md5(json_build_array(right_name, 
                                right_uri, notes)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_rights");
        }

    }

 
    public class ObjectCompositeHashCreators
    {
        string db_conn;
        HashHelper h;

        public ObjectCompositeHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
            h = new HashHelper(db_conn);
        }

        public void create_composite_object_hashes(int hash_type_id, string hash_type, string table_name)
        {
            string top_sql_string = @"Insert into sd.object_hashes 
                    (sd_oid, hash_type_id, hash_type, composite_hash)
                    select t.sd_oid, " + hash_type_id.ToString() + ", '" + hash_type + @"',  
                    md5(to_json(array_agg(t.record_hash ORDER BY t.record_hash))::varchar)
                    from sd." + table_name;

            h.CreateCompositeOjectHashes(top_sql_string, hash_type_id, hash_type);
        }


        public void create_full_data_object_hashes()
        {
            // needs to roll up, for any particular data object
            // all of the composite hashes plus any hash for a dataset property record,
            // plus the data object record itself
            string sql_string = @"update sd.data_objects d
            set object_full_hash = b.roll_up from
            (select sd_oid, 
             md5(to_json(array_agg(hash ORDER BY hash))::varchar) as roll_up
              from 
                (select sd_oid, composite_hash as hash   
                from sd.object_hashes
                union
                select sd_oid, record_hash as hash
                from sd.data_objects) h
             group by sd_oid) b
             where d.sd_oid = b.sd_oid";

            h.CreateFullDataObjectHashes(sql_string);
        }
    }


}
