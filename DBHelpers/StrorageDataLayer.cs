using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;

namespace DataHarvester
{
    public class StorageDataLayer : IStorageDataLayer
    {
        private string connString;
        private string ctg_connString;
        Credentials _credentials;

        /// <summary>
        /// Constructor is used to build the connection string, 
        /// using a credentials object that has the relevant credentials 
        /// from the app settings, themselves derived from a json file.
        /// </summary>
        /// 
        public StorageDataLayer(string database_name, Credentials credentials, int harvest_type_id)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();

            builder.Host = credentials.Host;
            builder.Username = credentials.Username;
            builder.Password = credentials.Password;

            builder.Database = (harvest_type_id == 3) ? "test" : database_name;

            connString = builder.ConnectionString;

            builder.Database = "ctg";
            ctg_connString = builder.ConnectionString;

            _credentials = credentials;
        }

        public string ConnString => connString;
        public string CTGConnString => ctg_connString;
        public Credentials Credentials => _credentials;


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


        public ulong StoreDatasetProperties(PostgreSQLCopyHelper<ObjectDataset> copyHelper, IEnumerable<ObjectDataset> entities)
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

