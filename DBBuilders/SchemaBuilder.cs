using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;

namespace DataHarvester
{
	public class SchemaBuilder
	{
		private string connString;
		private Source source;
		private StudyTableBuildersSD study_tablebuilder;
		private ObjectTableBuildersSD object_tablebuilder;

		public SchemaBuilder(string _connString, Source _source)
		{
			connString = _connString;
			source = _source;
			study_tablebuilder = new StudyTableBuildersSD(connString);
			object_tablebuilder = new ObjectTableBuildersSD(connString);
		}

		public void DeleteSDStudyTables()
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
			study_tablebuilder.drop_table("study_hashes");
		}

		public void DeleteSDObjectTables()
		{
			// dropping routines include 'if exists'
			// therefore can attempt to drop all of them
			object_tablebuilder.drop_table("data_objects");
			object_tablebuilder.drop_table("dataset_properties");
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
			object_tablebuilder.drop_table("object_hashes");
		}


		public void BuildNewSDStudyTables()
		{
			// these common to all databases

			study_tablebuilder.create_table_studies();
			study_tablebuilder.create_table_study_identifiers();
			study_tablebuilder.create_table_study_titles();
			study_tablebuilder.create_table_study_hashes();

			// these are database dependent
			if (source.has_study_topics) study_tablebuilder.create_table_study_topics();
			if (source.has_study_features) study_tablebuilder.create_table_study_features();
			if (source.has_study_contributors) study_tablebuilder.create_table_study_contributors();
			if (source.has_study_references) study_tablebuilder.create_table_study_references();
			if (source.has_study_relationships) study_tablebuilder.create_table_study_relationships();
			if (source.has_study_links) study_tablebuilder.create_table_study_links();
			if (source.has_study_ipd_available) study_tablebuilder.create_table_ipd_available();

		}


		public void BuildNewSDObjectTables()
		{
			// these common to all databases

			object_tablebuilder.create_table_data_objects();
			object_tablebuilder.create_table_object_instances();
			object_tablebuilder.create_table_object_titles();
			object_tablebuilder.create_table_object_hashes();

			// these are database dependent		

			if (source.has_dataset_properties) object_tablebuilder.create_table_dataset_properties();
			if (source.has_object_dates) object_tablebuilder.create_table_object_dates();
			if (source.has_object_relationships) object_tablebuilder.create_table_object_relationships();
			if (source.has_object_rights) object_tablebuilder.create_table_object_rights();
			if (source.has_object_pubmed_set)
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

