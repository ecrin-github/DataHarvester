using Dapper;
using Npgsql;


namespace DataHarvester
{
	public class StudyTableBuildersSD
	{
		string db_conn;

		public StudyTableBuildersSD(string _db_conn)
		{
			db_conn = _db_conn;
		}

		public void create_table_studies()
		{
			string sql_string = @"CREATE TABLE sd.studies(
				sd_id                  VARCHAR         PRIMARY KEY
              , hash_id                CHAR(32)        NULL
			  , display_title          VARCHAR         NULL
              , title_lang_code        VARCHAR         NULL default 'en'
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
              , record_hash            CHAR(32)        NULL
              , study_full_hash        CHAR(32)        NULL
			);
            CREATE INDEX studies_sd_id ON sd.studies(sd_id);
            CREATE INDEX studies_hash_id ON sd.studies(hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_identifiers()
		{
			string sql_string = @"CREATE TABLE sd.study_identifiers(
			    sd_id                  VARCHAR         NOT NULL
              , study_hash_id          CHAR(32)        NULL
			  , identifier_value       VARCHAR         NULL
			  , identifier_type_id     INT             NULL
			  , identifier_type        VARCHAR         NULL
			  , identifier_org_id      INT             NULL
			  , identifier_org         VARCHAR         NULL
			  , identifier_date        VARCHAR         NULL
			  , identifier_link        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_identifiers_study_sd_id ON sd.study_identifiers(sd_id);
            CREATE INDEX study_identifiers_study_hash_id ON sd.study_identifiers(study_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_relationships()
		{
			string sql_string = @"CREATE TABLE sd.study_relationships(
			    sd_id                  VARCHAR         NOT NULL
              , study_hash_id          CHAR(32)        NULL
			  , relationship_type_id   INT             NULL
			  , relationship_type      VARCHAR         NULL
			  , target_sd_id           VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_relationships_study_sd_id ON sd.study_relationships(sd_id);
            CREATE INDEX study_relationships_study_hash_id ON sd.study_relationships(study_hash_id);
			CREATE INDEX study_relationships_target_sd_id ON sd.study_relationships(target_sd_id);"; 

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_references()
		{
			string sql_string = @"CREATE TABLE sd.study_references(
				sd_id                  VARCHAR         NOT NULL
              , study_hash_id          CHAR(32)        NULL
			  , pmid                   VARCHAR         NULL
			  , citation               VARCHAR         NULL
			  , doi                    VARCHAR         NULL	
			  , comments               VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_references_study_sd_id ON sd.study_references(sd_id);
            CREATE INDEX study_references_study_hash_id ON sd.study_references(study_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_titles()
		{
			string sql_string = @"CREATE TABLE sd.study_titles(
				sd_id                  VARCHAR         NOT NULL
              , study_hash_id          CHAR(32)        NULL
			  , title_text             VARCHAR         NULL
			  , title_type_id          INT             NULL
			  , title_type             VARCHAR         NULL
			  , title_lang_code        VARCHAR         NOT NULL default 'en'
			  , lang_usage_id          INT             NOT NULL default 11
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , comparison_text        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_titles_study_sd_id ON sd.study_titles(sd_id);
            CREATE INDEX study_titles_study_hash_id ON sd.study_titles(study_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_contributors()
		{
			string sql_string = @"CREATE TABLE sd.study_contributors(
				sd_id                  VARCHAR         NOT NULL
              , study_hash_id          CHAR(32)        NULL
			  , contrib_type_id        INT             NULL
			  , contrib_type           VARCHAR         NULL
			  , is_individual          BOOLEAN         NULL
			  , organisation_id        INT             NULL
              , organisation_name      VARCHAR         NULL
			  , person_id              INT             NULL
			  , person_given_name      VARCHAR         NULL
			  , person_family_name     VARCHAR         NULL
			  , person_full_name       VARCHAR         NULL
			  , person_identifier      VARCHAR         NULL
			  , identifier_type        VARCHAR         NULL
			  , person_affiliation     VARCHAR         NULL
			  , affil_org_id           VARCHAR         NULL
			  , affil_org_id_type      VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_contributors_study_sd_id ON sd.study_contributors(sd_id);
            CREATE INDEX study_contributors_study_hash_id ON sd.study_contributors(study_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_topics()
		{
			string sql_string = @"CREATE TABLE sd.study_topics(
				sd_id                  VARCHAR         NOT NULL
              , study_hash_id          CHAR(32)        NULL
			  , topic_type_id          INT             NULL
			  , topic_type             VARCHAR         NULL
			  , topic_value            VARCHAR         NULL
			  , topic_ct_id            INT             NULL
			  , topic_ct               VARCHAR         NULL
			  , topic_ct_code          VARCHAR         NULL
			  , where_found            VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_topics_study_sd_id ON sd.study_topics(sd_id);
            CREATE INDEX study_topics_study_hash_id ON sd.study_topics(study_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_hashes()
		{
			string sql_string = @"CREATE TABLE sd.study_hashes(
				sd_id                  VARCHAR         NOT NULL
			  , study_hash_id          CHAR(32)        NULL
			  , hash_type_id           INT             NULL
			  , hash_type              VARCHAR         NULL
			  , composite_hash         CHAR(32)        NULL
			);
            CREATE INDEX study_hashes_study_sd_id ON sd.study_hashes(sd_id);
            CREATE INDEX study_hashes_study_hash_id ON sd.study_hashes(study_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}


	public class ObjectTableBuildersSD
	{
		string db_conn;

		public ObjectTableBuildersSD(string _db_conn)
		{
			db_conn = _db_conn;
		}


		public void create_table_data_objects()
		{
			string sql_string = @"CREATE TABLE sd.data_objects(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
              , study_hash_id          CHAR(32)        NULL
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
              , record_hash            CHAR(32)        NULL
              , object_full_hash       CHAR(32)        NULL
			);
            CREATE INDEX data_objects_study_sd_id_do_id ON sd.data_objects(sd_id, do_id);
            CREATE INDEX data_objects_object_hash_id ON sd.data_objects(object_hash_id);
            CREATE INDEX data_objects_study_hash_id ON sd.data_objects(study_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_dataset_properties()
		{
			string sql_string = @"CREATE TABLE sd.dataset_properties(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
			  , record_keys_type_id    INT             NULL 
			  , record_keys_type       VARCHAR         NULL     	
			  , record_keys_details    VARCHAR         NULL    
			  , identifiers_type_id    INT             NULL  
			  , identifiers_type  	   VARCHAR         NULL    
			  , identifiers_details    VARCHAR         NULL    
			  , consents_type_id       INT             NULL  
			  , consents_type          VARCHAR         NULL    
			  , consents_details       VARCHAR         NULL 
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX dataset_properties_sd_id_do_id ON sd.dataset_properties(sd_id, do_id);
            CREATE INDEX dataset_properties_object_hash_id ON sd.dataset_properties(object_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_dates()
		{
			string sql_string = @"CREATE TABLE sd.object_dates(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
			  , date_type_id           INT             NULL
			  , date_type              VARCHAR         NULL
			  , is_date_range          BOOLEAN         NULL default false
			  , date_as_string         VARCHAR         NULL
			  , start_year             INT             NULL
			  , start_month            INT             NULL
			  , start_day              INT             NULL
			  , end_year               INT             NULL
			  , end_month              INT             NULL
			  , end_day                INT             NULL
			  , details                VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX object_dates_sd_id_do_id ON sd.object_dates(sd_id, do_id);
            CREATE INDEX object_dates_object_hash_id ON sd.object_dates(object_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_instances()
		{
			string sql_string = @"CREATE TABLE sd.object_instances(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
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
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX object_instances_sd_id_do_id ON sd.object_instances(sd_id, do_id);
            CREATE INDEX object_instances_object_hash_id ON sd.object_instances(object_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_contributors()
		{
			string sql_string = @"CREATE TABLE sd.object_contributors(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
			  , contrib_type_id        INT             NULL
			  , contrib_type           VARCHAR         NULL
			  , is_individual          BOOLEAN         NULL
			  , organisation_id        INT             NULL
              , organisation_name      VARCHAR         NULL
			  , person_id              INT             NULL
			  , person_given_name      VARCHAR         NULL
			  , person_family_name     VARCHAR         NULL
			  , person_full_name       VARCHAR         NULL
			  , person_identifier      VARCHAR         NULL
			  , identifier_type        VARCHAR         NULL
			  , person_affiliation     VARCHAR         NULL
			  , affil_org_id           VARCHAR         NULL
			  , affil_org_id_type      VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX object_contributors_sd_id_do_id ON sd.object_contributors(sd_id, do_id);
            CREATE INDEX object_contributors_object_hash_id ON sd.object_contributors(object_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_titles()
		{
			string sql_string = @"CREATE TABLE sd.object_titles(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
			  , title_text             VARCHAR         NULL
			  , title_type_id          INT             NULL
			  , title_type             VARCHAR         NULL
			  , title_lang_code        VARCHAR         NOT NULL default 'en'
			  , lang_usage_id          INT             NOT NULL default 11
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , comparison_text        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX object_titles_sd_id_do_id ON sd.object_titles(sd_id, do_id);
            CREATE INDEX object_titles_object_hash_id ON sd.object_titles(object_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_object_topics()
		{
			string sql_string = @"CREATE TABLE sd.object_topics(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
			  , topic_type_id          INT             NULL
			  , topic_type             VARCHAR         NULL
			  , topic_value            VARCHAR         NULL
			  , topic_ct_id            INT             NULL
			  , topic_ct               VARCHAR         NULL
			  , topic_ct_code          VARCHAR         NULL
			  , where_found            VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX object_topics_sd_id_do_id ON sd.object_topics(sd_id, do_id);
            CREATE INDEX object_topics_object_hash_id ON sd.object_topics(object_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_languages()
		{
			string sql_string = @"CREATE TABLE sd.object_languages(
				sd_id                  VARCHAR         NOT NULL
			  , do_id                  INT             NOT NULL
              , object_hash_id         CHAR(32)        NULL
			  , lang_code              VARCHAR         NULL default 'en'
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX object_languages_sd_id_do_id ON sd.object_languages(sd_id, do_id);
            CREATE INDEX object_languages_object_hash_id ON sd.object_languages(object_hash_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_object_hashes()
		{
			string sql_string = @"CREATE TABLE sd.object_hashes(
				sd_id                  VARCHAR         NOT NULL
              , do_id                  INT             NULL
			  , study_hash_id          CHAR(32)        NULL
              , object_hash_id         CHAR(32)        NULL
			  , hash_type_id           INT             NULL
			  , hash_type              VARCHAR         NULL
              , composite_hash         CHAR(32)        NULL
			);
            CREATE INDEX object_hashes_sd_id_do_id ON sd.object_hashes(sd_id, do_id);
            CREATE INDEX object_hashes_object_hash_id ON sd.object_hashes(object_hash_id);
            CREATE INDEX object_hashes_study_hash_id ON sd.object_hashes(study_hash_id);";


			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}
}
