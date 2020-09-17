using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
    public class StudyHashCreators
    {
        string db_conn;

        public StudyHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
        }


        public void create_study_record_hashes()
        {
            string sql_string = @"Update sd.studies
              set record_hash = md5(json_build_array(display_title, brief_description, bd_contains_html,
              data_sharing_statement, dss_contains_html, study_start_year, study_start_month, study_type_id,
              study_status_id, study_enrolment, study_gender_elig_id, min_age,
              min_age_units_id, max_age, max_age_units_id)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_study_identifier_hashes()
        {
            string sql_string = @"Update sd.study_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, identifier_org_id,
              identifier_org, identifier_date, identifier_link)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_study_title_hashes()
        {
            string sql_string = @"Update sd.study_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, title_lang_code,
              lang_usage_id, is_default, comments)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_study_contributor_hashes()
        {
            string sql_string = @"Update sd.study_contributors
              set record_hash = md5(json_build_array(contrib_type_id, is_individual, organisation_id,
              organisation_name, person_id, person_given_name, person_family_name, person_full_name,
              person_identifier, identifier_type, person_affiliation, 
              affil_org_id, affil_org_id_type)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                 conn.Execute(sql_string);
            }
        }


        public void create_study_topic_hashes()
        {
            string sql_string = @"Update sd.study_topics
              set record_hash = md5(json_build_array(topic_type_id, topic_value, topic_ct_id,
              topic_ct_code, where_found)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_study_feature_hashes()
        {
            string sql_string = @"Update sd.study_features
              set record_hash = md5(json_build_array(feature_type_id, feature_value_id)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_study_reference_hashes()
        {
            string sql_string = @"Update sd.study_references
              set record_hash = md5(json_build_array(pmid, citation, doi, comments)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_study_relationship_hashes()
        {
            string sql_string = @"Update sd.study_relationships
              set record_hash = md5(json_build_array(relationship_type_id, target_sd_sid)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_study_link_hashes()
        {
            string sql_string = @"Update sd.study_links
              set record_hash = md5(json_build_array(link_label, link_url)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_ipd_available_hashes()
        {
            string sql_string = @"Update sd.study_ipd_available
              set record_hash = md5(json_build_array(ipd_id, ipd_type, ipd_url, ipd_comment)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }


  
    public class StudyCompositeHashCreators
    {
        string db_conn;

        public StudyCompositeHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
        }

        public void create_composite_study_hashes(int hash_type_id, string hash_type, string table_name)
        {
              string sql_string = @"Insert into sd.study_hashes 
              (sd_sid, hash_type_id, hash_type, composite_hash)
              select sd_sid, " + hash_type_id.ToString() + ", '" + hash_type + @"',  
              md5(to_json(array_agg(record_hash ORDER BY record_hash))::varchar)
              from sd." + table_name + " group by sd_sid;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        /*
        public void create_composite_dataobject_hashes()
        {
            string sql_string = @"Insert into sd.study_hashes 
              (sd_sid, hash_type_id, hash_type, composite_hash)
              select sd_sid, 10, 'data objects', 
              md5(to_json(array_agg(object_full_hash ORDER BY object_full_hash))::varchar)
              from sd.data_objects
              group by sd_sid;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
        */

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

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }


    public class ObjectHashCreators
    {
        string db_conn;

        public ObjectHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
        }

        public void create_object_record_hashes()
        {
            string sql_string = @"Update sd.data_objects
              set record_hash = md5(json_build_array(display_title, version, doi, doi_status_id, publication_year,
              object_class_id, object_type_id, managing_org_id, managing_org, access_type_id,
              access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
              add_study_topics)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_recordset_properties_hashes()
        {
            string sql_string = @"Update sd.dataset_properties
              set record_hash = md5(json_build_array(record_keys_type_id, record_keys_details,
              deident_type_id, deident_direct, deident_hipaa,
              deident_dates, deident_nonarr, deident_kanon, deident_details,
              consent_type_id, consent_noncommercial, consent_geog_restrict,
              consent_research_type, consent_genetic_only, consent_no_methods, consent_details)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_object_date_hashes()
        {
            string sql_string = @"Update sd.object_dates
              set record_hash = md5(json_build_array(date_type_id, is_date_range, date_as_string, 
              start_year, start_month, start_day, end_year, end_month, end_day,
              details)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_instance_hashes()
        {
            string sql_string = @"Update sd.object_instances
              set record_hash = md5(json_build_array(instance_type_id, repository_org_id, 
              repository_org, url, url_accessible, url_last_checked, 
              resource_type_id, resource_size, resource_size_units, resource_comments)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_title_hashes()
        {
            string sql_string = @"Update sd.object_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, title_lang_code, 
              lang_usage_id, is_default, comments)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_language_hashes()
        {
            string sql_string = @"Update sd.object_languages
              set record_hash = md5(json_build_array(lang_code)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_object_contributor_hashes()
        {
            string sql_string = @"Update sd.object_contributors
              set record_hash = md5(json_build_array(contrib_type_id, is_individual, organisation_id,
              organisation_name, person_id, person_given_name, person_family_name, person_full_name,
              person_identifier, identifier_type, person_affiliation, 
              affil_org_id, affil_org_id_type)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_topic_hashes()
        {
            string sql_string = @"Update sd.object_topics
              set record_hash = md5(json_build_array(topic_type_id, topic_value, topic_ct_id,
              topic_ct_code, where_found)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_comment_hashes()
        {
            string sql_string = @"Update sd.object_comments
              set record_hash = md5(json_build_array(ref_type, ref_source, pmid, pmid_version,
              notes)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_description_hashes()
        {
            string sql_string = @"Update sd.object_descriptions
              set record_hash = md5(json_build_array(description_type_id, label, description_text, lang_code,
              contains_html)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_identifier_hashes()
        {
            string sql_string = @"Update sd.object_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, 
                                identifier_org_id, identifier_org, identifier_date)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_object_db_link_hashes()
        {
            string sql_string = @"Update sd.object_db_links
              set record_hash = md5(json_build_array(db_sequence, db_name, 
              id_in_db)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_object_publication_type_hashes()
        {
            string sql_string = @"Update sd.object_publication_types
              set record_hash = md5(json_build_array(type_name)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_object_relationship_hashes()
        {
            string sql_string = @"Update sd.object_publication_types
              set record_hash = md5(json_build_array(relationship_type_id,
                                target_sd_oid)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_object_right_hashes()
        {
            string sql_string = @"Update sd.object_publication_types
              set record_hash = md5(json_build_array(right_name, 
                                right_uri, notes)::varchar);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }

 
    public class ObjectCompositeHashCreators
    {
        string db_conn;

        public ObjectCompositeHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
        }

        public void create_composite_object_hashes(int hash_type_id, string hash_type, string table_name)
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_oid, hash_type_id, hash_type, composite_hash)
              select sd_oid, " + hash_type_id.ToString() + ", '" + hash_type + @"',  
              md5(to_json(array_agg(record_hash ORDER BY record_hash))::varchar)
              from sd." + table_name + " group by sd_oid;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
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
             where d.sd_oid = b.sd_oid;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }

}
