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

        // Inserts the base Studyobject, i.e. with all the  
        // singleton properties, in the database.
        public void StoreStudy(StudyInDB st_db, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Insert<StudyInDB>(st_db);
            }
        }

        public ulong StoreStudyIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper,
                              IEnumerable<StudyIdentifier> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreStudyTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper,
                              IEnumerable<StudyTitle> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreStudyRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper,
                               IEnumerable<StudyRelationship> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }

        }


        public ulong StoreStudyReferences(PostgreSQLCopyHelper<StudyReference> copyHelper,
                             IEnumerable<StudyReference> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreStudyContributors(PostgreSQLCopyHelper<StudyContributor> copyHelper,
                                       IEnumerable<StudyContributor> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreStudyTopics(PostgreSQLCopyHelper<StudyTopic> copyHelper,
                          IEnumerable<StudyTopic> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }

        public ulong StoreStudyFeatures(PostgreSQLCopyHelper<StudyFeature> copyHelper,
                          IEnumerable<StudyFeature> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreStudyLinks(PostgreSQLCopyHelper<StudyLink> copyHelper,
                        IEnumerable<StudyLink> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }

        public ulong StoreStudyIpdInfo(PostgreSQLCopyHelper<AvailableIPD> copyHelper,
                         IEnumerable<AvailableIPD> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }



        public ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper,
                       IEnumerable<DataObject> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreDatasetProperties(PostgreSQLCopyHelper<ObjectDataset> copyHelper,
                        IEnumerable<ObjectDataset> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreObjectTitles(PostgreSQLCopyHelper<ObjectTitle> copyHelper,
                        IEnumerable<ObjectTitle> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreObjectDates(PostgreSQLCopyHelper<ObjectDate> copyHelper,
                                IEnumerable<ObjectDate> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreObjectInstances(PostgreSQLCopyHelper<ObjectInstance> copyHelper,
                                IEnumerable<ObjectInstance> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of description records associated with each Data.

        public ulong StoreObjectDescriptions(PostgreSQLCopyHelper<ObjectDescription> copyHelper,
                               IEnumerable<ObjectDescription> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }



        // Inserts the set of 'databank' accession records associated with each Data,
        // including any linked ClinicalTrials.gov NCT numbers.

        public ulong StoreObjectAcessionNumbers(PostgreSQLCopyHelper<ObjectDBLink> copyHelper,
                               IEnumerable<ObjectDBLink> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of publication type records associated with each Data.

        public ulong StorePublicationTypes(PostgreSQLCopyHelper<ObjectPublicationType> copyHelper,
                               IEnumerable<ObjectPublicationType> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }



        // Inserts the set of any comments records associated with each Data.

        public ulong StoreObjectComments(PostgreSQLCopyHelper<ObjectComment> copyHelper,
                                 IEnumerable<ObjectComment> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the set of identifiers records associated with each Data.

        public ulong StoreObjectIdentifiers(PostgreSQLCopyHelper<ObjectIdentifier> copyHelper,
                                    IEnumerable<ObjectIdentifier> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        // Inserts the contributor (person or organisation) records for each Data Object.

        public ulong StoreObjectContributors(PostgreSQLCopyHelper<ObjectContributor> copyHelper,
                                    IEnumerable<ObjectContributor> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }

        // Inserts the set of keyword records associated with each Data Object.

        public ulong StoreObjectTopics(PostgreSQLCopyHelper<ObjectTopic> copyHelper,
                                     IEnumerable<ObjectTopic> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }

        // Inserts any rights records associated with each Data Object.

        public ulong StoreObjectRights(PostgreSQLCopyHelper<ObjectRight> copyHelper,
                                      IEnumerable<ObjectRight> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }

        // Inserts related object records associated with each Data Object.

        public ulong StoreObjectRelationships(PostgreSQLCopyHelper<ObjectRelationship> copyHelper,
                                       IEnumerable<ObjectRelationship> entities, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public void StoreCitationObject(CitationObjectInDB ctob, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Insert<CitationObjectInDB>(ctob);
            }
        }

    }
}

