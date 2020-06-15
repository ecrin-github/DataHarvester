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
	public class SDBuilder
	{
		private string connString;
		private Source source;

		public SDBuilder(string _connString, Source _source)
		{
			connString = _connString;
			source = _source;
		}

		public void DeleteSDStudyTables()
		{
			// dropping routines include 'if exists'
			// therefore can attempt to drop all of them
			
			StudyTableDroppers dropper = new StudyTableDroppers(connString);
			dropper.drop_table_studies();
			dropper.drop_table_study_identifiers();
			dropper.drop_table_study_titles();
			dropper.drop_table_study_contributors();
			dropper.drop_table_study_topics();
			dropper.drop_table_study_relationships();
			dropper.drop_table_study_references();
			dropper.drop_table_study_hashes();
			dropper.drop_table_study_links();
			dropper.drop_table_study_ipd_available();
		}

		public void DeleteSDObjectTables()
		{   
			// dropping routines include 'if exists'
			// therefore can attempt to drop all of them

			ObjectTableDroppers dropper = new ObjectTableDroppers(connString);
			dropper.drop_table_data_objects();
			dropper.drop_table_dataset_properties();
			dropper.drop_table_object_dates();
			dropper.drop_table_object_instances();
			dropper.drop_table_object_titles();
			dropper.drop_table_object_languages();
			dropper.drop_table_object_hashes();
			dropper.drop_table_object_corrections();
			dropper.drop_table_object_descriptions();
			dropper.drop_table_object_identifiers();
			dropper.drop_table_object_links();
			dropper.drop_table_object_public_types();
		}


		public void BuildNewSDStudyTables()
		{
			// these common to all databases

			StudyTableBuildersSD builder = new StudyTableBuildersSD(connString);
			builder.create_table_studies();
			builder.create_table_study_identifiers();
			builder.create_table_study_titles();
			builder.create_table_study_hashes();

			// these are database dependent
			if (source.has_study_topics) builder.create_table_study_topics();
			if (source.has_study_features) builder.create_table_study_features();
			if (source.has_study_contributors) builder.create_table_study_contributors();
			if (source.has_study_references) builder.create_table_study_references();
			if (source.has_study_relationships) builder.create_table_study_relationships();
			if (source.has_study_links) builder.create_table_study_links();
			if (source.has_study_ipd_available) builder.create_table_ipd_available();

		}


		public void BuildNewSDObjectTables()
		{
			// these common to all databases

			ObjectTableBuildersSD builder = new ObjectTableBuildersSD(connString);
			builder.create_table_data_objects();
			builder.create_table_object_instances();
			builder.create_table_object_titles();	
			builder.create_table_object_hashes();

			// these are database dependent		

			if (source.has_dataset_properties) builder.create_table_dataset_properties();
			if (source.has_object_dates) builder.create_table_object_dates();
			if (source.has_object_languages) builder.create_table_object_languages();
			if (source.has_object_pubmed_set)
			{
				builder.create_table_object_contributors();
				builder.create_table_object_topics();
				builder.create_table_object_corrections();
				builder.create_table_object_descriptions();
				builder.create_table_object_identifiers();
				builder.create_table_object_links();
				builder.create_table_object_public_types();
			}
		}

	}
}

