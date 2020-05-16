using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.DBHelpers
{
    public class StudyTableDroppers
    {
		string db_conn;

		public StudyTableDroppers(string _db_conn)
		{
			db_conn = _db_conn;
		}
		
		public void drop_table_studies()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.studies;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_identifiers()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_identifiers;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_titles()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_titles;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_contributors()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_contributors;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_topics()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_topics;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_relationships()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_relationships;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_references()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_references;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_hashes()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_hashes;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

	public class ObjectTableDroppers
	{
		string db_conn;

		public ObjectTableDroppers(string _db_conn)
		{
			db_conn = _db_conn;
		}
		
		public void drop_table_data_objects()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.data_objects;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_dataset_properties()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.dataset_properties;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_dates()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_dates;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_instances()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_instances;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_titles()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_titles;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_contributors()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_contributors;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_topics()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_topics;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_languages()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_languages;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_hashes()
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_hashes;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

}
