using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;
using DataHarvester.biolincc;

namespace DataHarvester
{
	public class DataLayer
	{
		private string connString;
		private string ctg_connString;
		private string username;
		private string password;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// </summary>
		/// 
		public DataLayer(string database_name)
		{
				IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];
			builder.Database = database_name;

			connString = builder.ConnectionString;

			builder.Database = "ctg";
			ctg_connString = builder.ConnectionString;

			username = builder.Username;
			password = builder.Password;
		}

		public string ConnString => connString;
		public string CTGConnString => ctg_connString;

		public string Username => username;
		public string Password => password;


		// Inserts the base Studyobject, i.e. with all the  
		// singleton properties, in the database.
		public void StoreStudy(StudyInDB st_db)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Insert<StudyInDB>(st_db);
			}
		}

		public ulong StoreStudyIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper, IEnumerable<StudyIdentifier> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreStudyTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper, IEnumerable<StudyTitle> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreStudyRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper, IEnumerable<StudyRelationship> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}

		}


		public ulong StoreStudyReferences(PostgreSQLCopyHelper<StudyReference> copyHelper, IEnumerable<StudyReference> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreStudyContributors(PostgreSQLCopyHelper<StudyContributor> copyHelper, IEnumerable<StudyContributor> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreStudyTopics(PostgreSQLCopyHelper<StudyTopic> copyHelper, IEnumerable<StudyTopic> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyFeatures(PostgreSQLCopyHelper<StudyFeature> copyHelper, IEnumerable<StudyFeature> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreStudyLinks(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyIpdInfo(PostgreSQLCopyHelper<AvailableIPD> copyHelper, IEnumerable<AvailableIPD> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}



		public ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper, IEnumerable<DataObject> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreDatasetProperties(PostgreSQLCopyHelper<DataSetProperties> copyHelper, IEnumerable<DataSetProperties> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreObjectTitles(PostgreSQLCopyHelper<ObjectTitle> copyHelper,
						IEnumerable<ObjectTitle> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreObjectDates(PostgreSQLCopyHelper<ObjectDate> copyHelper,
						IEnumerable<ObjectDate> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public ulong StoreObjectInstances(PostgreSQLCopyHelper<ObjectInstance> copyHelper,
						IEnumerable<ObjectInstance> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		// Inserts the set of description records associated with each Data.

		public ulong StoreObjectDescriptions(PostgreSQLCopyHelper<ObjectDescription> copyHelper, IEnumerable<ObjectDescription> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}



		// Inserts the set of 'databank' accession records associated with each Data,
		// including any linked ClinicalTrials.gov NCT numbers.

		public ulong StoreObjectAcessionNumbers(PostgreSQLCopyHelper<ObjectDBLink> copyHelper, IEnumerable<ObjectDBLink> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		// Inserts the set of publication type records associated with each Data.

		public ulong StorePublicationTypes(PostgreSQLCopyHelper<ObjectPublicationType> copyHelper, IEnumerable<ObjectPublicationType> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}



		// Inserts the set of any comments records associated with each Data.

		public ulong StoreObjectComments(PostgreSQLCopyHelper<ObjectComment> copyHelper, IEnumerable<ObjectComment> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		// Inserts the set of identifiers records associated with each Data.

		public ulong StoreObjectIdentifiers(PostgreSQLCopyHelper<ObjectIdentifier> copyHelper, IEnumerable<ObjectIdentifier> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		// Inserts the contributor (person or organisation) records for each Data Object.

		public ulong StoreObjectContributors(PostgreSQLCopyHelper<ObjectContributor> copyHelper, IEnumerable<ObjectContributor> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		// Inserts the set of keyword records associated with each Data Object.

		public ulong StoreObjectTopics(PostgreSQLCopyHelper<ObjectTopic> copyHelper, IEnumerable<ObjectTopic> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

        // Inserts any rights records associated with each Data Object.

		public ulong StoreObjectRights(PostgreSQLCopyHelper<ObjectRight> copyHelper, IEnumerable<ObjectRight> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		// Inserts related object records associated with each Data Object.

		public ulong StoreObjectRelationships(PostgreSQLCopyHelper<ObjectRelationship> copyHelper, IEnumerable<ObjectRelationship> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		
		public void StoreCitationObject(CitationObjectInDB ctob)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Insert<CitationObjectInDB>(ctob);
			}
		}

	}
}

