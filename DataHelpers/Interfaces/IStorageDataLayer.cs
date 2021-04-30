using PostgreSQLCopyHelper;
using System.Collections.Generic;

namespace DataHarvester
{
    public interface IStorageDataLayer
    {
        void StoreCitationObject(CitationObjectInDB ctob, string connString);
        void StoreStudy(StudyInDB st_db, string connString);

        ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper, IEnumerable<DataObject> entities, string connString);
        ulong StoreDatasetProperties(PostgreSQLCopyHelper<ObjectDataset> copyHelper, IEnumerable<ObjectDataset> entities, string connString);
        ulong StoreObjectAcessionNumbers(PostgreSQLCopyHelper<ObjectDBLink> copyHelper, IEnumerable<ObjectDBLink> entities, string connString);
        ulong StoreObjectComments(PostgreSQLCopyHelper<ObjectComment> copyHelper, IEnumerable<ObjectComment> entities, string connString);
        ulong StoreObjectContributors(PostgreSQLCopyHelper<ObjectContributor> copyHelper, IEnumerable<ObjectContributor> entities, string connString);
        ulong StoreObjectDates(PostgreSQLCopyHelper<ObjectDate> copyHelper, IEnumerable<ObjectDate> entities, string connString);
        ulong StoreObjectDescriptions(PostgreSQLCopyHelper<ObjectDescription> copyHelper, IEnumerable<ObjectDescription> entities, string connString);
        ulong StoreObjectIdentifiers(PostgreSQLCopyHelper<ObjectIdentifier> copyHelper, IEnumerable<ObjectIdentifier> entities, string connString);
        ulong StoreObjectInstances(PostgreSQLCopyHelper<ObjectInstance> copyHelper, IEnumerable<ObjectInstance> entities, string connString);
        ulong StoreObjectRelationships(PostgreSQLCopyHelper<ObjectRelationship> copyHelper, IEnumerable<ObjectRelationship> entities, string connString);
        ulong StoreObjectRights(PostgreSQLCopyHelper<ObjectRight> copyHelper, IEnumerable<ObjectRight> entities, string connString);
        ulong StoreObjectTitles(PostgreSQLCopyHelper<ObjectTitle> copyHelper, IEnumerable<ObjectTitle> entities, string connString);
        ulong StoreObjectTopics(PostgreSQLCopyHelper<ObjectTopic> copyHelper, IEnumerable<ObjectTopic> entities, string connString);
        ulong StorePublicationTypes(PostgreSQLCopyHelper<ObjectPublicationType> copyHelper, IEnumerable<ObjectPublicationType> entities, string connString);
        ulong StoreStudyContributors(PostgreSQLCopyHelper<StudyContributor> copyHelper, IEnumerable<StudyContributor> entities, string connString);
        ulong StoreStudyFeatures(PostgreSQLCopyHelper<StudyFeature> copyHelper, IEnumerable<StudyFeature> entities, string connString);
        ulong StoreStudyIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper, IEnumerable<StudyIdentifier> entities, string connString);
        ulong StoreStudyIpdInfo(PostgreSQLCopyHelper<AvailableIPD> copyHelper, IEnumerable<AvailableIPD> entities, string connString);
        ulong StoreStudyLinks(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities, string connString);
        ulong StoreStudyReferences(PostgreSQLCopyHelper<StudyReference> copyHelper, IEnumerable<StudyReference> entities, string connString);
        ulong StoreStudyRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper, IEnumerable<StudyRelationship> entities, string connString);
        ulong StoreStudyTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper, IEnumerable<StudyTitle> entities, string connString);
        ulong StoreStudyTopics(PostgreSQLCopyHelper<StudyTopic> copyHelper, IEnumerable<StudyTopic> entities, string connString);
    }
}