using Dapper;
using Npgsql;


namespace DataHarvester
{
	public static class StudyTableBuilders
	{
		public static void create_table_studies(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.studies(
					sd_id                  VARCHAR         PRIMARY KEY
				  , display_title          VARCHAR         NULL
				  , brief_description      VARCHAR         NULL
                  , bd_contains_html       BOOLEAN         NULL	default false
				  , data_sharing_statement VARCHAR         NULL
                  , dss_contains_html      BOOLEAN         NULL	default false
				  , study_start_year       INT             NULL
				  , study_start_month      INT             NULL
				  , study_type_id          INT             NULL
				  , study_type             VARCHAR         NULL
				  , study_status_id        INT             NULL
				  , study_status           VARCHAR         NULL
				  , study_enrolment        INT             NULL
				  , study_gender_elig_id   INT             NULL
				  , study_gender_elig      VARCHAR         NULL
				  , min_age                INT             NULL
				  , min_age_units_id       INT             NULL
				  , min_age_units          VARCHAR         NULL
				  , max_age                INT             NULL
				  , max_age_units_id       INT             NULL
				  , max_age_units          VARCHAR         NULL
				  , datetime_of_data_fetch TIMESTAMPTZ     NULL
				);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_study_identifiers(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.study_identifiers(
				  sd_id                  VARCHAR         NOT NULL
				, identifier_value       VARCHAR         NULL
				, identifier_type_id     INT             NULL
				, identifier_type        VARCHAR         NULL
				, identifier_org_id      INT             NULL
				, identifier_org         VARCHAR         NULL
				, identifier_date        VARCHAR         NULL
				, identifier_link        VARCHAR         NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_study_relationships(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.study_relationships(
			    sd_id                  VARCHAR         NOT NULL
			  , relationship_type_id   INT             NULL
			  , relationship_type      VARCHAR         NULL
			  , target_sd_id           VARCHAR         NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_study_references(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.study_references(
				sd_id                  VARCHAR         NOT NULL
			  , pmid                   VARCHAR         NULL
			  , citation               VARCHAR         NULL
			  , doi                    VARCHAR         NULL	
			  , comments               VARCHAR         NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_study_titles(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.study_titles(
				sd_id                  VARCHAR         NOT NULL
			  , title_text             VARCHAR         NULL
			  , title_type_id          INT             NULL
			  , title_type             VARCHAR         NULL
			  , title_lang_code        VARCHAR         NOT NULL default 'en'
			  , lang_usage_id          INT             NOT NULL default 11
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , comparison_text        VARCHAR         NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_study_jsonb(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.study_jsonb(
				sd_id                  VARCHAR         NOT NULL
			  , study_fields           JSONB           NULL
			  , study_identifiers      JSONB           NULL
			  , study_titles           JSONB           NULL
			  , study_references       JSONB           NULL
			  , study_relationships    JSONB           NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}


	public static class ObjectTableBuilders
	{
		public static void create_table_data_objects(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.data_objects(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
			  , display_title          VARCHAR         NULL
			  , doi                    VARCHAR         NULL 
			  , doi_status_id          INT             NULL
			  , publication_year       INT             NULL
			  , object_class_id        INT             NULL
			  , object_class           VARCHAR         NULL
			  , object_type_id         INT             NULL
			  , object_type            VARCHAR         NULL
			  , managing_org_id        INT             NULL
			  , managing_org           VARCHAR         NULL
			  , access_type_id         INT             NULL
			  , access_type            VARCHAR         NULL
			  , access_details         VARCHAR         NULL
			  , access_details_url     VARCHAR         NULL
			  , url_last_checked       DATE            NULL
			  , add_study_contribs     BOOLEAN         NULL
			  , add_study_topics       BOOLEAN         NULL
			  , datetime_of_data_fetch TIMESTAMPTZ     NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_dataset_properties(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.dataset_properties(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
			  , record_keys_type_id    INT             NULL 
			  , record_keys_type       VARCHAR         NULL     	
			  , record_keys_details    VARCHAR         NULL    
			  , identifiers_type_id    INT             NULL  
			  , identifiers_type  	   VARCHAR         NULL    
			  , identifiers_details    VARCHAR         NULL    
			  , consents_type_id       INT             NULL  
			  , consents_type          VARCHAR         NULL    
			  , consents_details       VARCHAR         NULL    
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_object_dates(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.object_dates(
				sd_id                 VARCHAR          NOT NULL
			  , do_id                 INT              NOT NULL
			  , date_type_id          INT              NULL
			  , date_type             VARCHAR          NULL
			  , is_date_range         BOOLEAN          NULL default false
			  , date_as_string        VARCHAR          NULL
			  , start_year            INT              NULL
			  , start_month           INT              NULL
			  , start_day             INT              NULL
			  , end_year              INT              NULL
			  , end_month             INT              NULL
			  , end_day               INT              NULL
			  , details               VARCHAR          NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_object_instances(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.object_instances(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
			  , instance_type_id       INT             NOT NULL  default 1
			  , instance_type          VARCHAR         NULL default 'Full Resource'
			  , repository_org_id      INT             NULL
			  , repository_org         VARCHAR         NULL
			  , url                    VARCHAR         NULL
			  , url_accessible         BOOLEAN         NULL
			  , url_last_checked       DATE            NULL
			  , resource_type_id       INT             NULL
			  , resource_type          VARCHAR         NULL
			  , resource_size          VARCHAR         NULL
			  , resource_size_units    VARCHAR         NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_object_titles(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.object_titles(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
			  , title_text             VARCHAR         NULL
			  , title_type_id          INT             NULL
			  , title_type             VARCHAR         NULL
			  , title_lang_code        VARCHAR         NOT NULL default 'en'
			  , lang_usage_id          INT             NOT NULL default 11
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , comparison_text        VARCHAR         NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public static void create_table_object_jsonb(string db_conn)
		{
			string sql_string = @"CREATE TABLE sd.object_jsonb(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
			  , object_fields          JSONB           NULL
			  , object_datasets        JSONB           NULL
			  , object_titles          JSONB           NULL
			  , object_instances       JSONB           NULL
			  , object_dates           JSONB           NULL
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}
}
