using System;
using System.Collections.Generic;
using Npgsql;
using System.Text;
using Dapper.Contrib.Extensions;
using PostgreSQLCopyHelper;
using Microsoft.Extensions.Configuration;

namespace DataHarvester.ctg
{
	public class CTGDataLayer
	{

		private string _connString;
		private string _folder_base;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public CTGDataLayer()
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			builder.Database = "ctg";
			builder.SearchPath = "sd";
			_connString = builder.ConnectionString;

			_folder_base = settings["folder_base"];

			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
		}
		

		/// <summary>
		/// Uses the _folder_base value from the settings file and
		/// combines it with a date string to provide a parent directory
		/// for the processed files.
		/// </summary>
		public string GetFolderBase()
		{
			return _folder_base;
		}


		public void StoreStudy(StudyInDB sdb)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
			{
				Conn.Insert<StudyInDB>(sdb);
			}
		}


		public ulong StoreIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper, IEnumerable<StudyIdentifier> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper, IEnumerable<StudyTitle> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreContributors(PostgreSQLCopyHelper<StudyContributor> copyHelper, IEnumerable<StudyContributor> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreReferences(PostgreSQLCopyHelper<StudyReference> copyHelper, IEnumerable<StudyReference> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreLinks(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreIPDInfo(PostgreSQLCopyHelper<AvailableIPD> copyHelper, IEnumerable<AvailableIPD> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreTopics(PostgreSQLCopyHelper<StudyTopic> copyHelper, IEnumerable<StudyTopic> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreFeatures(PostgreSQLCopyHelper<StudyFeature> copyHelper, IEnumerable<StudyFeature> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper, IEnumerable<StudyRelationship> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper, IEnumerable<DataObject> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreObjectTitles(PostgreSQLCopyHelper<ObjectTitle> copyHelper, IEnumerable<ObjectTitle> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreObjectDates(PostgreSQLCopyHelper<ObjectDate> copyHelper, IEnumerable<ObjectDate> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreObjectInstances(PostgreSQLCopyHelper<ObjectInstance> copyHelper, IEnumerable<ObjectInstance> entities)
		{
			using (var conn = new NpgsqlConnection(_connString))
			{
				conn.Open();
				// Returns count of rows written 
				return copyHelper.SaveAll(conn, entities);
			}
		}

	}
}
