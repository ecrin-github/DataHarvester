using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.DBHelpers
{
    public static class StudyTableDroppers
    {
		public static void drop_table_studies(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.studies;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_study_identifiers(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_identifiers;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_study_titles(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_titles;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_study_relationships(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_relationships;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_study_references(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_references;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_study_jsonb(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.study_hashes;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

	public static class ObjectTableDroppers
	{
		public static void drop_table_data_objects(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.data_objects;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_dataset_properties(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.dataset_properties;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_object_dates(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_dates;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_object_instances(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_instances;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_object_titles(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_titles;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public static void drop_table_object_jsonb(string db_conn)
		{
			string sql_string = @"DROP TABLE IF EXISTS sd.object_hashes;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

}
