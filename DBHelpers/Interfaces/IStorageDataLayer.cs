using PostgreSQLCopyHelper;
using System.Collections.Generic;

namespace DataHarvester
{
    public interface IStorageDataLayer
    {
        string ConnString { get; }
        Credentials Credentials { get; }
        string CTGConnString { get; }

        void StoreCitationObject(CitationObjectInDB ctob);
        ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper, IEnumerable<DataObject> entities);
        ulong StoreDatasetProperties(PostgreSQLCopyHelper<ObjectDataset> copyHelper, IEnumerable<ObjectDataset> entities);
        ulong StoreObjectAcessionNumbers(PostgreSQLCopyHelper<ObjectDBLink> copyHelper, IEnumerable<ObjectDBLink> entities);
        ulong StoreObjectComments(PostgreSQLCopyHelper<ObjectComment> copyHelper, IEnumerable<ObjectComment> entities);
        ulong StoreObjectContributors(PostgreSQLCopyHelper<ObjectContributor> copyHelper, IEnumerable<ObjectContributor> entities);
        ulong StoreObjectDates(PostgreSQLCopyHelper<ObjectDate> copyHelper, IEnumerable<ObjectDate> entities);
        ulong StoreObjectDescriptions(PostgreSQLCopyHelper<ObjectDescription> copyHelper, IEnumerable<ObjectDescription> entities);
        ulong StoreObjectIdentifiers(PostgreSQLCopyHelper<ObjectIdentifier> copyHelper, IEnumerable<ObjectIdentifier> entities);
        ulong StoreObjectInstances(PostgreSQLCopyHelper<ObjectInstance> copyHelper, IEnumerable<ObjectInstance> entities);
        ulong StoreObjectRelationships(PostgreSQLCopyHelper<ObjectRelationship> copyHelper, IEnumerable<ObjectRelationship> entities);
        ulong StoreObjectRights(PostgreSQLCopyHelper<ObjectRight> copyHelper, IEnumerable<ObjectRight> entities);
        ulong StoreObjectTitles(PostgreSQLCopyHelper<ObjectTitle> copyHelper, IEnumerable<ObjectTitle> entities);
        ulong StoreObjectTopics(PostgreSQLCopyHelper<ObjectTopic> copyHelper, IEnumerable<ObjectTopic> entities);
        ulong StorePublicationTypes(PostgreSQLCopyHelper<ObjectPublicationType> copyHelper, IEnumerable<ObjectPublicationType> entities);
        void StoreStudy(StudyInDB st_db);
        ulong StoreStudyContributors(PostgreSQLCopyHelper<StudyContributor> copyHelper, IEnumerable<StudyContributor> entities);
        ulong StoreStudyFeatures(PostgreSQLCopyHelper<StudyFeature> copyHelper, IEnumerable<StudyFeature> entities);
        ulong StoreStudyIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper, IEnumerable<StudyIdentifier> entities);
        ulong StoreStudyIpdInfo(PostgreSQLCopyHelper<AvailableIPD> copyHelper, IEnumerable<AvailableIPD> entities);
        ulong StoreStudyLinks(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities);
        ulong StoreStudyReferences(PostgreSQLCopyHelper<StudyReference> copyHelper, IEnumerable<StudyReference> entities);
        ulong StoreStudyRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper, IEnumerable<StudyRelationship> entities);
        ulong StoreStudyTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper, IEnumerable<StudyTitle> entities);
        ulong StoreStudyTopics(PostgreSQLCopyHelper<StudyTopic> copyHelper, IEnumerable<StudyTopic> entities);
    }
}