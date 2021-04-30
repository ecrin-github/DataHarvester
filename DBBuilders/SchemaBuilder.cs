using Serilog;

namespace DataHarvester
{
    public class SchemaBuilder
    {
        private Source _source;
        private ILogger _logger;
        private StudyTableBuilder study_tablebuilder;
        private ObjectTableBuilder object_tablebuilder;

        public SchemaBuilder(Source source, ILogger logger)
        {
            _source = source;
            _logger = logger;
            study_tablebuilder = new StudyTableBuilder(source.db_conn);
            object_tablebuilder = new ObjectTableBuilder(source.db_conn);
        }

        public void RecreateTables()
        {
            if (_source.has_study_tables)
            {
                DeleteStudyTables();
                _logger.Information("Existing study tables deleted");

                BuildNewStudyTables();
                _logger.Information("Study tables recreated");
            }

            DeleteObjectTables();
            _logger.Information("Existing object tables deleted");

            BuildNewObjectTables();
            _logger.Information("Object tables recreated");
        }


        private void DeleteStudyTables()
        {
            // dropping routines include 'if exists'
            // therefore can attempt to drop all of them
            study_tablebuilder.drop_table("studies");
            study_tablebuilder.drop_table("study_identifiers");
            study_tablebuilder.drop_table("study_titles");
            study_tablebuilder.drop_table("study_contributors");
            study_tablebuilder.drop_table("study_topics");
            study_tablebuilder.drop_table("study_features");
            study_tablebuilder.drop_table("study_relationships");
            study_tablebuilder.drop_table("study_references");
            study_tablebuilder.drop_table("study_links");
            study_tablebuilder.drop_table("study_ipd_available");
        }

        private void DeleteObjectTables()
        {
            // dropping routines include 'if exists'
            // therefore can attempt to drop all of them
            object_tablebuilder.drop_table("data_objects");
            object_tablebuilder.drop_table("object_datasets");
            object_tablebuilder.drop_table("object_dates");
            object_tablebuilder.drop_table("object_instances");
            object_tablebuilder.drop_table("object_titles");
            object_tablebuilder.drop_table("object_contributors");
            object_tablebuilder.drop_table("object_topics");
            object_tablebuilder.drop_table("object_comments");
            object_tablebuilder.drop_table("object_descriptions");
            object_tablebuilder.drop_table("object_identifiers");
            object_tablebuilder.drop_table("object_db_links");
            object_tablebuilder.drop_table("object_publication_types");
            object_tablebuilder.drop_table("object_relationships");
            object_tablebuilder.drop_table("object_rights");
            object_tablebuilder.drop_table("citation_objects");
        }


        private void BuildNewStudyTables()
        {
            // these common to all databases

            study_tablebuilder.create_table_studies();
            study_tablebuilder.create_table_study_identifiers();
            study_tablebuilder.create_table_study_titles();

            // these are database dependent
            if (_source.has_study_topics) study_tablebuilder.create_table_study_topics();
            if (_source.has_study_features) study_tablebuilder.create_table_study_features();
            if (_source.has_study_contributors) study_tablebuilder.create_table_study_contributors();
            if (_source.has_study_references) study_tablebuilder.create_table_study_references();
            if (_source.has_study_relationships) study_tablebuilder.create_table_study_relationships();
            if (_source.has_study_links) study_tablebuilder.create_table_study_links();
            if (_source.has_study_ipd_available) study_tablebuilder.create_table_ipd_available();

        }


        private void BuildNewObjectTables()
        {
            // these common to all databases

            object_tablebuilder.create_table_data_objects();
            object_tablebuilder.create_table_object_instances();
            object_tablebuilder.create_table_object_titles();

            // these are database dependent		

            if (_source.has_object_datasets) object_tablebuilder.create_table_object_datasets();
            if (_source.has_object_dates) object_tablebuilder.create_table_object_dates();
            if (_source.has_object_relationships) object_tablebuilder.create_table_object_relationships();
            if (_source.has_object_rights) object_tablebuilder.create_table_object_rights();
            if (_source.has_object_pubmed_set)
            {
                object_tablebuilder.create_table_citation_objects();
                object_tablebuilder.create_table_object_contributors();
                object_tablebuilder.create_table_object_topics();
                object_tablebuilder.create_table_object_comments();
                object_tablebuilder.create_table_object_descriptions();
                object_tablebuilder.create_table_object_identifiers();
                object_tablebuilder.create_table_object_db_links();
                object_tablebuilder.create_table_object_publication_types();
            }
        }

    }
}

