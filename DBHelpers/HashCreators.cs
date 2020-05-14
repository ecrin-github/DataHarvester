using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.DBHelpers
{
    public static class StudyHashCreators
    {
        public static void CreateStudyIdHashes(string db_conn, int source_id)
        {
            string sql_string = @"Update sd.studies
              set hash_id = md5(json_build_array('" + source_id.ToString() + "' || sd_id, display_title)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public static void CreateStudyRecordHashes(string db_conn)
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


        public static void CreateStudyIdentifierHashes(string db_conn)
        {
            string sql_string = @"Update sd.study_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, identifier_org_id,
              identifier_org, identifier_date, identifier_link)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public static void CreateStudyTitleHashes(string db_conn)
        {
            string sql_string = @"Update sd.study_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, title_lang_code,
              lang_usage_id, is_default, comments)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public static void CreateStudyReferenceHashes(string db_conn)
        {
            string sql_string = @"Update sd.study_references
              set record_hash = md5(json_build_array(pmid, citation, doi, comments)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }


    public static class StudyHashInserters
    {
        public static void InsertStudyHashesIntoStudyIdentifiers(string db_conn)
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

        public static void InsertStudyHashesIntoStudyTitles(string db_conn)
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

        public static void InsertStudyHashesIntoStudyReferences(string db_conn)
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


    public static class StudyCompositeHashCreators
    {
        public static void CreateCompositeStudyIdentifierHashes(string db_conn)
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

        public static void CreateCompositeStudyTitleHashes(string db_conn)
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

        public static void CreateCompositeStudyReferenceHashes(string db_conn)
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

        public static void CreateCompositeDataObjectHashes(string db_conn)
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

        public static void CreateFullStudyHashes(string db_conn)
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


    public static class ObjectHashCreators
    {
        public static void CreateObjectIdHashes(string db_conn)
        {
            string sql_string = @"Update sd.data_objects d
              set hash_id = md5(json_build_array(s.hash_id, d.display_title)::varchar)::char(32)
              from sd.studies s
              where d.sd_id = s.sd_id";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void CreateObjectRecordHashes(string db_conn)
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


        public static void CreateRecordsetPropertiesHashes(string db_conn)
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

        public static void CreateObjectDateHashes(string db_conn)
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

        public static void CreateObjectInstanceHashes(string db_conn)
        {
            string sql_string = @"Update sd.object_instances
              set record_hash = md5(json_build_array(instance_type_id, repository_org_id, repository_org, url, url_accessible,
              url_last_checked, resource_type_id, resource_size, resource_size_units)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void CreateObjectTitledHashes(string db_conn)
        {
            string sql_string = @"Update sd.object_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, title_lang_code, lang_usage_id,
              is_default, comments)::varchar)::char(32);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void InsertStudyHashesIntoDataObjects(string db_conn)
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

        public static void InsertObjectHashesIntoDatasetProperties(string db_conn)
        {
            string sql_string = @"Update sd.dataset_properties t
              set study_hash_id = d.study_hash_id,
              object_hash_id = d.hash_id
              from sd.data_objects d 
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void InsertObjectHashesIntoObjectInstances(string db_conn)
        {
            string sql_string = @"Update sd.object_instances t
              set study_hash_id = d.study_hash_id,
              object_hash_id = d.hash_id
              from sd.data_objects d 
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void InsertObjectHashesIntoObjectTitles(string db_conn)
        {
            string sql_string = @"Update sd.object_titles t
              set study_hash_id = d.study_hash_id,
              object_hash_id = d.hash_id
              from sd.data_objects d
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void InsertObjectHashesIntoObjectDates(string db_conn)
        {
            string sql_string = @"Update sd.object_dates t
              set study_hash_id = d.study_hash_id,
              object_hash_id = d.hash_id
              from sd.data_objects d 
              where t.sd_id = d.sd_id
              and t.do_id = d.do_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }

    public static class ObjectCompositeHashCreators
    {

        public static void CreateCompositeObjectInstanceHashes(string db_conn)
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_id, do_id, study_hash_id, object_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, do_id, study_hash_id, object_hash_id, 11, 'instances', 
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.object_instances
              group by sd_id, do_id, study_hash_id, object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void CreateCompositeObjectTitlesHashes(string db_conn)
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_id, do_id, study_hash_id, object_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, do_id, study_hash_id, object_hash_id, 12, 'titles', 
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.object_titles
              group by sd_id, do_id, study_hash_id, object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void CreateCompositeObjectDatesHashes(string db_conn)
        {
            string sql_string = @"Insert into sd.object_hashes 
              (sd_id, do_id, study_hash_id, object_hash_id, hash_type_id, hash_type, composite_hash)
              select sd_id, do_id, study_hash_id, object_hash_id, 13, 'dates', 
              md5(to_json(array_agg(record_hash))::varchar)::char(32)
              from sd.object_dates
              group by sd_id, do_id, study_hash_id, object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void CreateFullDataObjectHashes(string db_conn)
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
                from sd.dataset_properties
                union
                select sd_id, do_id, study_hash_id, hash_id as object_hash_id, record_hash as hash
                from sd.data_objects) h
             group by sd_id, do_id, study_hash_id, object_hash_id) b
             where d.sd_id = b.sd_id
             and d.do_id = b.do_id
             and d.study_hash_id = b.study_hash_id
             and d.hash_id  = b.object_hash_id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }

}
