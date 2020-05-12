using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.DBHelpers
{
    public static class StudyJsonBBuilders
    {
        public static void CreateStudyJsonB(string db_conn)
        {
            string sql_string = @"Insert into sd.study_jsonb
              (sd_id, study_fields)
              select sd_id, to_jsonb(display_title, brief_description,
              bd_contains_html, data_sharing_statement, dss_contains_html,
              study_start_year, study_start_month, study_type_id, study_type,
              study_status_id, study_status, study_enrolment, 
              study_gender_elig_id, study_gender_elig,
              min_age, min_age_units_id, min_age_units, 
              max_age, max_age_units_id, max_age_units, 
              datetime_of_data_fetch)
              from sd.studies";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void CreateStudyidentifiersJsonB(string db_conn)
        {
            string sql_string = @"Update sd.study_jsonb j
              set study_identifiers = jsonb_build_array(
                  select to_jsonb(identifier_value, 
                  identifier_type_id, identifier_type, identifier_org_id, 
                  identifier_org, identifier_date, identifier_link)
                  from sd.study_identifiers s
                  where j.sd_id = s.sd_id
                  order by identifier_value, identifier_org) t";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }


    public static class ObjectJsonBBuilders
    {
        public static void CreateDataObjectJsonB(string db_conn)
        {
            string sql_string = @"Insert into sd.study_jsonb
              (sd_id, do_id, study_fields)
              select sd_id, do_id, to_jsonb(display_title, doi,
              doi_status_id, publication_year, object_class_id, object_class,
              object_type_id, object_type, managing_org_id, managing_org,
              access_type_id, access_type, access_details, 
              access_details_url, url_last_checked,
              add_study_contribs, add_study_topics, 
              datetime_of_data_fetch)
              from sd.data_objects";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void CreateDatasetPropertiesJsonB(string db_conn)
        {
            string sql_string = @"Insert into sd.study_jsonb
              (sd_id, do_id, study_fields)
              select sd_id, do_id, to_jsonb(display_title, doi,
              doi_status_id, publication_year, object_class_id, object_class,
              object_type_id, object_type, managing_org_id, managing_org,
              access_type_id, access_type, access_details, 
              access_details_url, url_last_checked,
              add_study_contribs, add_study_topics, 
              datetime_of_data_fetch)
              from sd.data_objects";
             
            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


    }

}
