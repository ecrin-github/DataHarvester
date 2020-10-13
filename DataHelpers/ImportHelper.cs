using Dapper;
using Dapper.Contrib;
using Dapper.Contrib.Extensions;
using Npgsql;

namespace DataHarvester
{
	class ImportTableCreator
	{
		string connstring;

		public ImportTableCreator(string _connstring)
		{
			connstring = _connstring;
		}


		public void ExecuteSQL(string sql_string)
		{
			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		// These 4 tables recreatd at each import, but left in the database 
		// until the following import.

		public void CreateStudyRecsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.import_study_recs;
            CREATE TABLE sd.import_study_recs(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , status                 INT             NOT NULL
              , study_rec_status       INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateStudyAttsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.import_study_changed_atts;
            CREATE TABLE sd.import_study_changed_atts(
                sd_sid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NOT NULL
              , status                 INT             NOT NULL
              , composite_hash         CHAR(32)        NULL
			);
            CREATE INDEX import_studysid_hash ON sd.import_study_changed_atts(sd_sid, hash_type_id);";
			ExecuteSQL(sql_string);
		}


		public void CreateObjectRecsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.import_object_recs;
            CREATE TABLE sd.import_object_recs(
                sd_oid                  VARCHAR         NOT NULL PRIMARY KEY
              , status                  INT             NOT NULL
              , object_rec_status       INT             NULL
              , object_dataset_status   INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateObjectAttsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.import_object_changed_atts;
            CREATE TABLE sd.import_object_changed_atts(
                sd_oid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NOT NULL
              , status                 INT             NOT NULL
              , composite_hash         CHAR(32)        NULL
			);
            CREATE INDEX import_object_sid_hash ON sd.import_object_changed_atts(sd_oid, hash_type_id);";
			ExecuteSQL(sql_string);
		}
	}


	class ImportTableManager
	{
		string connstring;
		bool ad_studies_exists;
		bool ad_data_objects_exists;
		bool ad_dataset_properties_exists;
		bool ad_study_hashes_exists;
		bool ad_object_hashes_exists;

		public ImportTableManager(string _connstring)
		{
			connstring = _connstring;
		}

		public void ExecuteSQL(string sql_string)
		{
			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public bool CheckADTableExistsSQL(string table_name)
		{
			string sql_string = @"SELECT to_regclass('ad." + table_name + "')::varchar";

			using (var conn = new NpgsqlConnection(connstring))
			{
				string res = conn.ExecuteScalar<string>(sql_string);
				return (res != null);
			}
		}


		public void CheckExistanceOfADTables()
		{
			ad_studies_exists = CheckADTableExistsSQL("studies");
			ad_data_objects_exists = CheckADTableExistsSQL("data_objects");
			ad_dataset_properties_exists = CheckADTableExistsSQL("dataset_properties");
			ad_study_hashes_exists = CheckADTableExistsSQL("study_hashes");
			ad_object_hashes_exists = CheckADTableExistsSQL("object_hashes");
		}


		public void IdentifyNewStudies()
		{
			string sql_string = "";
			if (ad_studies_exists)
			{
				sql_string = @"INSERT INTO sd.import_study_recs (sd_sid, status)
                SELECT s.sd_sid, 1 from sd.studies s
                LEFT JOIN ad.studies a
                on s.sd_sid = a.sd_sid 
                WHERE a.sd_sid is null;";
			}
			else
			{
				sql_string = @"INSERT INTO sd.import_study_recs (sd_sid, status)
                SELECT s.sd_sid, 1 from sd.studies s;";
			}
			ExecuteSQL(sql_string);
		}


		public void IdentifyEditedStudies()
		{
			if (ad_studies_exists)
			{
				string sql_string = @"INSERT INTO sd.import_study_recs (sd_sid, status)
			    SELECT s.sd_sid, 2 from sd.studies s
			    INNER JOIN ad.studies a
			    on s.sd_sid = a.sd_sid
                where s.study_full_hash <> a.study_full_hash;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyIdenticalStudies()
		{
			if (ad_studies_exists)
			{
				string sql_string = @"INSERT INTO sd.import_study_recs (sd_sid, status)
				SELECT s.sd_sid, 3 from sd.studies s
				INNER JOIN ad.studies a
				on s.sd_sid = a.sd_sid
                where s.study_full_hash = a.study_full_hash;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyDeletedStudies()
		{
			if (ad_studies_exists)
			{
				string sql_string = @"INSERT INTO sd.import_study_recs(sd_sid, status)
			    SELECT a.sd_sid, 4 from ad.studies a
			    LEFT JOIN sd.studies s
			    on a.sd_sid = s.sd_sid
			    WHERE s.sd_sid is null;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyNewDataObjects()
		{
			string sql_string = ""; 
			if (ad_data_objects_exists)
			{
				sql_string = @"INSERT INTO sd.import_object_recs(sd_oid, status)
			    SELECT d.sd_oid, 1 from sd.data_objects d
			    LEFT JOIN ad.data_objects a
                on d.sd_oid = a.sd_oid
			    WHERE a.sd_oid is null;";
			}
			else
            {
				sql_string = @"INSERT INTO sd.import_object_recs(sd_oid, status)
			    SELECT d.sd_oid, 1 from sd.data_objects d;";
            }
			ExecuteSQL(sql_string);
		}


		public void IdentifyEditedDataObjects()
		{
			if (ad_data_objects_exists)
			{
				string sql_string = @"INSERT INTO sd.import_object_recs(sd_oid, status)
				SELECT d.sd_oid, 2 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_oid = a.sd_oid
                WHERE d.object_full_hash <> a.object_full_hash;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyIdenticalDataObjects()
		{
			if (ad_data_objects_exists)
			{
				string sql_string = @"INSERT INTO sd.import_object_recs(sd_oid, status)
				SELECT d.sd_oid, 3 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_oid = a.sd_oid
                WHERE d.object_full_hash = a.object_full_hash;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyDeletedDataObjects()
		{
			if (ad_data_objects_exists)
			{
				string sql_string = @"INSERT INTO sd.import_object_recs(sd_oid, status)
			    SELECT a.sd_oid, 4 from sd.data_objects a
			    LEFT JOIN sd.data_objects d
			    on a.sd_oid = d.sd_oid
			    WHERE d.sd_oid is null;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyChangedStudyRecs()
		{
			if (ad_studies_exists)
			{
				string sql_string = @"with t as (
                select s.sd_sid
                from sd.studies s
			    INNER JOIN ad.studies a
			    on s.sd_sid = a.sd_sid
                where s.record_hash <> a.record_hash)
                UPDATE sd.import_study_recs c
                SET study_rec_status = 2
                from t
			    WHERE t.sd_sid = c.sd_sid;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyChangedObjectRecs()
		{
			if (ad_data_objects_exists)
			{
				string sql_string = @"with t as (
                select s.sd_oid
                from sd.data_objects s
			    INNER JOIN ad.data_objects a
			    on s.sd_oid = a.sd_oid
                where s.record_hash <> a.record_hash)
                UPDATE sd.import_object_recs c
			    SET object_rec_status = 2
                FROM t
			    WHERE t.sd_oid = c.sd_oid;";
				ExecuteSQL(sql_string);
			}
		}

		public void IdentifyChangedDatasetRecs()
		{
			if (ad_dataset_properties_exists)
			{
				string sql_string = @"with t as (
                select s.sd_oid
                from sd.dataset_properties s
			    INNER JOIN ad.dataset_properties a
			    on s.sd_oid = a.sd_oid
                where s.record_hash <> a.record_hash)
                UPDATE sd.import_object_recs c
			    SET object_dataset_status = 4
                FROM t
			    WHERE t.sd_oid = c.sd_oid;";
				ExecuteSQL(sql_string);
			}
		}

		public void IdentifyChangedStudyAtts()
		{
			// Storeas the sd_id and hash type of all changed composite hash values
			// in edited records - indicates that one or more of the attributes has changed.
			if (ad_study_hashes_exists)
			{
				string sql_string = @"insert into sd.import_study_changed_atts
                (sd_sid, hash_type_id, status, composite_hash)
                select s.sd_sid, s.hash_type_id, 2, s.composite_hash
                from sd.study_hashes s
                INNER JOIN 
                   (SELECT sd_sid FROM sd.import_study_recs 
                    WHERE status = 2) r
                on s.sd_sid = r.sd_sid
			    INNER JOIN ad.study_hashes a
			    on s.sd_sid = a.sd_sid
                and s.hash_type_id = a.hash_type_id
                where s.composite_hash <> a.composite_hash;";
				ExecuteSQL(sql_string);
			}
		}

		public void IdentifyNewStudyAtts()
		{
			// Stores the sd_id and hash type of a new ad_sid / hash type combinations,
			// indicates that one or more of new types of attributes have been added.

			if (ad_study_hashes_exists)
			{
				string sql_string = @"insert into sd.import_study_changed_atts
			    (sd_sid, hash_type_id, status, composite_hash)
                select s.sd_sid, s.hash_type_id, 1, s.composite_hash
                from sd.study_hashes s
                INNER JOIN 
                   (SELECT sd_sid FROM sd.import_study_recs 
                    WHERE status = 2) r
                on s.sd_sid = r.sd_sid
			    LEFT JOIN ad.study_hashes a
			    on s.sd_sid = a.sd_sid
                and s.hash_type_id = a.hash_type_id
                where a.sd_sid is null;";
				ExecuteSQL(sql_string);
			}
		}

		public void IdentifyDeletedStudyAtts()
		{
			// Stores the sd_id and hash type of deleted ad_sid / hash type combinations,
			// indicates that one or more types of attributes have disappeared.

			if (ad_study_hashes_exists)
			{
				string sql_string = @"insert into sd.import_study_changed_atts
			    (sd_sid, hash_type_id, status)
                select a.sd_sid, a.hash_type_id, 4
                from sd.study_hashes s
                INNER JOIN 
                   (SELECT sd_sid FROM sd.import_study_recs 
                    WHERE status = 2) r
                on s.sd_sid = r.sd_sid
			    RIGHT JOIN ad.study_hashes a
			    on a.sd_sid = s.sd_sid
                and a.hash_type_id = s.hash_type_id
                where s.sd_sid is null;";
				ExecuteSQL(sql_string);
			}
		}

		public void IdentifyChangedObjectAtts()
		{
			if (ad_object_hashes_exists)
			{
				string sql_string = @"insert into sd.import_object_changed_atts
                (sd_oid, hash_type_id, status, composite_hash)
                select s.sd_oid, s.hash_type_id, 2, s.composite_hash
                from sd.object_hashes s
                INNER JOIN 
                   (SELECT sd_oid FROM sd.import_object_recs 
                    WHERE status = 2) r
                on s.sd_oid = r.sd_oid
			    INNER JOIN ad.object_hashes a
                on s.sd_oid = a.sd_oid
			    and s.hash_type_id = a.hash_type_id
                where s.composite_hash <> a.composite_hash;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyNewObjectAtts()
		{
			if (ad_object_hashes_exists)
			{
				string sql_string = @"insert into sd.import_object_changed_atts
                (sd_oid, hash_type_id, status, composite_hash)
                select s.sd_oid, s.hash_type_id, 1, s.composite_hash
                from sd.object_hashes s
                INNER JOIN 
                   (SELECT sd_oid FROM sd.import_object_recs 
                    WHERE status = 2) r
                on s.sd_oid = r.sd_oid
			    LEFT JOIN ad.object_hashes a
			    on s.sd_oid = a.sd_oid
                and s.hash_type_id = a.hash_type_id
                where a.sd_oid is null;";
				ExecuteSQL(sql_string);
			}
		}


		public void IdentifyDeletedObjectAtts()
		{
			if (ad_object_hashes_exists)
			{
				string sql_string = @"insert into sd.import_object_changed_atts
                (sd_oid, hash_type_id, status)
                select a.sd_oid, a.hash_type_id, 4
                from sd.object_hashes s
                INNER JOIN 
                   (SELECT sd_oid FROM sd.import_object_recs 
                    WHERE status = 2) r
                on s.sd_oid = r.sd_oid
			    RIGHT JOIN ad.object_hashes a
			    on a.sd_oid = s.sd_oid
                and a.hash_type_id = s.hash_type_id
                where s.sd_oid is null;";
				ExecuteSQL(sql_string);
			}
		}
	}
}
