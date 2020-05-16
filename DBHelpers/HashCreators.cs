using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.DBHelpers
{
    public class StudyHashCreators
    {
        string db_conn;

        public StudyHashCreators(string _db_conn)
        {
            db_conn = _db_conn;
        }


        public void CreateStudyIdHashes(int source_id)
        {
            string sql_string = @"Update sd.studies
              set hash_id = md5(json_build_array('" + source_id.ToString() +
                                      "' || sd_id, display_title)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void CreateStudyRecordHashes()
        {
            string sql_string = @"Update sd.studies
              set record_hash = md5(json_build_array(display_title, brief_description, bd_contains_html,
              data_sharing_statement, dss_contains_html, study_start_year, study_start_month, study_type_id,
              study_status_id, study_enrolment, study_gender_elig_id, min_age,
              min_age_units_id, max_age, max_age_units_id)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void CreateStudyIdentifierHashes()
        {
            string sql_string = @"Update sd.study_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, identifier_org_id,
              identifier_org, identifier_date, identifier_link)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void CreateStudyTitleHashes()
        {
            string sql_string = @"Update sd.study_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, title_lang_code,
              lang_usage_id, is_default, comments)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void CreateStudyContributorHashes()
        {
            string sql_string = @"Update sd.study_contributors
              set record_hash = md5(json_build_array(contrib_type_id, is_individual, organisation_id,
              organisation_name, person_id, person_given_name, person_family_name, person_full_name,
              person_identifier, identifier_type, person_affiliation, 
              affil_org_id, affil_org_id_type)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                 conn.Execute(sql_string);
            }
        }


        public void CreateStudyTopicHashes()
        {
            string sql_string = @"Update sd.study_topics
              set record_hash = md5(json_build_array(topic_type_id, topic_value, topic_ct_id,
              topic_ct_code, where_found)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }



        public void CreateStudyReferenceHashes()
        {
            string sql_string = @"Update sd.study_references
              set record_hash = md5(json_build_array(pmid, citation, doi, comments)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }


    public class StudyHashInserters
    {
        string db_conn;

        public StudyHashInserters(string _db_conn)
        {
            db_conn = _db_conn;
        }


        public void InsertStudyHashesIntoStudyIdentifiers()
        {
            string sql_string = @"Update sd.study_identifiers t
              set study_hash_id = s.hash_id 
              from sd.studies s 
              where t.sd_id = s.sd_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertStudyHashesIntoStudyTitles()
        {
            string sql_string = @"Update sd.study_titles t
              set study_hash_id = s.hash_id 
              from sd.studies s 
              where t.sd_id = s.sd_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertStudyHashesIntoStudyContributors()
        {
            string sql_string = @"Update sd.study_contributors t
              set study_hash_id = s.hash_id 
              from sd.studies s 
              where t.sd_id = s.sd_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertStudyHashesIntoStudyTopics()
        {
            string sql_string = @"Update sd.study_topics t
              set study_hash_id = s.hash_id 
              from sd.studies s 
              where t.sd_id = s.sd_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertStudyHashesIntoStudyReferences()
        {
            string sql_string = @"Update sd.study_references t
              set study_hash_id = s.hash_id 
              from sd.studies s 
              where t.sd_id = s.sd_id;";

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


        public void CreateCompositeStudyIdentifierHashes()
        {
            string sql_string = @"Insert into sd.study_hashes 
              (sd_id, study_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, study_hash_id, 11, 'identifiers', 
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.study_identifiers
              group by sd_id, study_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateCompositeStudyTitleHashes()
        {
            string sql_string = @"Insert into sd.study_hashes 
              (sd_id, study_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, study_hash_id, 12, 'titles', 
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.study_titles
              group by sd_id, study_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateCompositeStudyContributorHashes()
        {
            string sql_string = @"Insert into sd.study_hashes 
              (sd_id, study_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, study_hash_id, 15, 'contributors',
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.study_contributors
              group by sd_id, study_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateCompositeStudyTopicHashes()
        {
            string sql_string = @"Insert into sd.study_hashes 
              (sd_id, study_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, study_hash_id, 14, 'topics',
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.study_topics
              group by sd_id, study_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                 conn.Execute(sql_string);
            }
        }


        public void CreateCompositeStudyReferenceHashes()
        {
            string sql_string = @"Insert into sd.study_hashes 
              (sd_id, study_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, study_hash_id, 17, 'references',
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.study_references
              group by sd_id, study_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateCompositeDataObjectHashes()
        {
            string sql_string = @"Insert into sd.study_hashes 
              (sd_id, study_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, study_hash_id, 10, 'data objects', 
              md5(to_json(array_agg(object_full_hash))::varchar)::char(32)
              from sd.data_objects
              group by sd_id, study_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateFullStudyHashes()
        {
            string sql_string = @"update sd.studies s
            set study_full_hash = b.rollup
            from (select sd_id, study_hash_id, md5(to_json(array_agg(h.hash))::varchar)::char(32) as rollup
                  from 
                     (select sd_id, study_hash_id, composite_hash as hash 
                      from sd.study_hashes
                      union
                      select sd_id, hash_id as study_hash_id, record_hash as hash
                      from sd.studies) h
	              group by sd_id, study_hash_id) b
            where 
            s.sd_id = b.sd_id
            and s.hash_id = b.study_hash_id";

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


        public void CreateObjectIdHashes()
        {
            string sql_string = @"Update sd.data_objects d
              set object_hash_id = md5(json_build_array(s.hash_id, d.display_title)::varchar)::char(32)
              from sd.studies s
              where d.sd_id = s.sd_id";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateObjectRecordHashes()
        {
            string sql_string = @"Update sd.data_objects
              set record_hash = md5(json_build_array(display_title, doi, doi_status_id, publication_year,
              object_class_id, object_type_id, managing_org_id, managing_org, access_type_id,
              access_details, access_details_url, url_last_checked, add_study_contribs,
              add_study_topics)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void CreateRecordsetPropertiesHashes()
        {
            string sql_string = @"Update sd.dataset_properties
              set record_hash = md5(json_build_array(record_keys_type_id, record_keys_details, 
              identifiers_type_id, identifiers_details, consents_type_id, 
              consents_details)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateObjectDateHashes()
        {
            string sql_string = @"Update sd.object_dates
              set record_hash = md5(json_build_array(date_type_id, is_date_range, date_as_string, 
              start_year, start_month, start_day, end_year, end_month, end_day,
              details)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateObjectInstanceHashes()
        {
            string sql_string = @"Update sd.object_instances
              set record_hash = md5(json_build_array(instance_type_id, repository_org_id, repository_org, url, url_accessible,
              url_last_checked, resource_type_id, resource_size, resource_size_units)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateObjectTitledHashes()
        {
            string sql_string = @"Update sd.object_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, title_lang_code, lang_usage_id,
              is_default, comments)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }

    public class ObjectHashInserters
    {
        string db_conn;

        public ObjectHashInserters(string _db_conn)
        {
            db_conn = _db_conn;
        }


        public void InsertStudyHashesIntoDataObjects()
        {
            string sql_string = @"Update sd.data_objects t
              set study_hash_id = s.hash_id 
              from sd.studies s 
              where t.sd_id = s.sd_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertObjectHashesIntoDatasetProperties()
        {
            string sql_string = @"Update sd.dataset_properties t
              set object_hash_id = d.object_hash_id
              from sd.data_objects d 
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertObjectHashesIntoObjectInstances()
        {
            string sql_string = @"Update sd.object_instances t
              set object_hash_id = d.object_hash_id
              from sd.data_objects d 
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertObjectHashesIntoObjectTitles()
        {
            string sql_string = @"Update sd.object_titles t
              set object_hash_id = d.object_hash_id
              from sd.data_objects d
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void InsertObjectHashesIntoObjectDates()
        {
            string sql_string = @"Update sd.object_dates t
              set object_hash_id = d.object_hash_id
              from sd.data_objects d 
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

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

        // sould only be 0 or 1 one per data object but it makes 
        // further processing a little easier
        public void CreateCompositeDatasetPropertiesHashes()
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_id, do_id, study_hash_id, object_hash_id, hash_type_id, hash_type, composite_hash)
              select t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id, 60, 'dataset properties', 
              md5(to_json(array_agg(t.record_hash))::varchar)::char(32)
              from sd.dataset_properties t
              inner join sd.data_objects d
              on t.sd_id = d.sd_id
              and t.do_id = d.do_id
              group by t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void CreateCompositeObjectInstanceHashes()
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_id, do_id, study_hash_id, object_hash_id, hash_type_id, hash_type, composite_hash)
              select t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id, 51, 'instances', 
              md5(to_json(array_agg(t.record_hash))::varchar)::char(32)
              from sd.object_instances t
              inner join sd.data_objects d
              on t.sd_id = d.sd_id
              and t.do_id = d.do_id
              group by t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateCompositeObjectTitlesHashes()
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_id, do_id, study_hash_id, object_hash_id, hash_type_id, hash_type, composite_hash)
              select t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id, 52, 'titles', 
              md5(to_json(array_agg(t.record_hash))::varchar)::char(32)
              from sd.object_titles t
              inner join sd.data_objects d
              on t.sd_id = d.sd_id
              and t.do_id = d.do_id
              group by t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateCompositeObjectDatesHashes()
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_id, do_id, study_hash_id, object_hash_id, hash_type_id, hash_type, composite_hash)
              select t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id, 53, 'dates', 
              md5(to_json(array_agg(t.record_hash))::varchar)::char(32)
              from sd.object_dates t
              inner join sd.data_objects d
              on t.sd_id = d.sd_id
              and t.do_id = d.do_id
              group by t.sd_id, t.do_id, d.study_hash_id, t.object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void CreateFullDataObjectHashes()
        {
            // needs to roll up, for any particular data object
            // all of the composite hashes plus any hash for a dataset property record,
            // plus the data object record itself
            string sql_string = @"update sd.data_objects d
            set object_full_hash = b.roll_up from
            (select sd_id, do_id, study_hash_id, object_hash_id, 
             md5(to_json(array_agg(h.hash))::varchar)::char(32) as roll_up
              from 
                (select sd_id, do_id, study_hash_id, object_hash_id, composite_hash as hash  
                from sd.object_hashes
                union
                select sd_id, do_id, study_hash_id, object_hash_id, record_hash as hash
                from sd.data_objects) h
             group by sd_id, do_id, study_hash_id, object_hash_id) b
             where d.sd_id = b.sd_id
             and d.do_id = b.do_id
             and d.study_hash_id = b.study_hash_id
             and d.object_hash_id  = b.object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }

}
