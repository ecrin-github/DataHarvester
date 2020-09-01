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
	public class HashBuilder
	{
		private string connString;
		private Source source;

		public HashBuilder(string _connString, Source _source)
		{
			connString = _connString;
			source = _source;
		}

		public void CreateStudyHashes()
		{
			StudyHashCreators hashcreator = new StudyHashCreators(connString);
			hashcreator.create_study_record_hashes();
			hashcreator.create_study_identifier_hashes();
			hashcreator.create_study_title_hashes();

			// these are database dependent
			if (source.has_study_topics) hashcreator.create_study_topic_hashes();
			if (source.has_study_features) hashcreator.create_study_feature_hashes();
			if (source.has_study_contributors) hashcreator.create_study_contributor_hashes();
			if (source.has_study_references) hashcreator.create_study_reference_hashes();
			if (source.has_study_relationships) hashcreator.create_study_relationship_hashes();
			if (source.has_study_links) hashcreator.create_study_link_hashes();
			if (source.has_study_ipd_available) hashcreator.create_ipd_available_hashes();
		}

		public void CreateStudyCompositeHashes()
		{
			StudyCompositeHashCreators hashcreator = new StudyCompositeHashCreators(connString);
			hashcreator.create_composite_study_hashes(11, "identifiers", "study_identifiers");
			hashcreator.create_composite_study_hashes(12, "titles", "study_titles");

			// these are database dependent
			if (source.has_study_features) hashcreator.create_composite_study_hashes(13, "features", "study_features");
			if (source.has_study_topics) hashcreator.create_composite_study_hashes(14, "topics", "study_topics");
			if (source.has_study_contributors) hashcreator.create_composite_study_hashes(15, "contributors", "study_contributors");
			if (source.has_study_relationships) hashcreator.create_composite_study_hashes(16, "relationships", "study_relationships");
			if (source.has_study_references) hashcreator.create_composite_study_hashes(17, "references", "study_references");
			if (source.has_study_links) hashcreator.create_composite_study_hashes(18, "study links", "study_links");
			if (source.has_study_ipd_available) hashcreator.create_composite_study_hashes(19, "ipd available", "study_ipd_available");
		}


		public void CreateDataObjectHashes()
		{
    		ObjectHashCreators hashcreator = new ObjectHashCreators(connString);
			hashcreator.create_object_record_hashes();
			hashcreator.create_object_instance_hashes();
			hashcreator.create_object_title_hashes();

			// these are database dependent		
			if (source.has_dataset_properties) hashcreator.create_recordset_properties_hashes();
			if (source.has_object_dates) hashcreator.create_object_date_hashes();
			if (source.has_object_languages) hashcreator.create_object_language_hashes();
			if (source.has_object_pubmed_set)
			{
				hashcreator.create_object_contributor_hashes();
				hashcreator.create_object_topic_hashes();
				hashcreator.create_object_correction_hashes();
				hashcreator.create_object_description_hashes();
				hashcreator.create_object_identifier_hashes();
				hashcreator.create_object_link_hashes();
				hashcreator.create_object_public_type_hashes();
			}

		}


		public void CreateObjectCompositeHashes()
		{
			ObjectCompositeHashCreators hashcreator = new ObjectCompositeHashCreators(connString);
			hashcreator.create_composite_object_hashes(51, "instances", "object_instances");
			hashcreator.create_composite_object_hashes(52, "titles", "object_titles");

			// these are database dependent		
			if (source.has_dataset_properties) hashcreator.create_composite_object_hashes(50, "datasets", "dataset_properties");
			if (source.has_object_dates) hashcreator.create_composite_object_hashes(53, "dates", "object_dates");
			if (source.has_object_languages) hashcreator.create_composite_object_hashes(58, "languages", "object_languages");
			if (source.has_object_pubmed_set)
			{
				hashcreator.create_composite_object_hashes(54, "topics", "object_topics");
				hashcreator.create_composite_object_hashes(55, "contributors", "object_contributors");
				hashcreator.create_composite_object_hashes(57, "descriptions", "object_descriptions");
				hashcreator.create_composite_object_hashes(60, "links", "object_links");
				hashcreator.create_composite_object_hashes(61, "corrections", "object_corrections");
				hashcreator.create_composite_object_hashes(62, "public types", "object_public_types");
				hashcreator.create_composite_object_hashes(63, "identifiers", "object_identifiers");
			}
			// objects must be fully rolled up first..
			hashcreator.create_full_data_object_hashes();

			StudyCompositeHashCreators studyhashcreator = new StudyCompositeHashCreators(connString);
			studyhashcreator.create_composite_dataobject_hashes();
			studyhashcreator.create_full_study_hashes();
		}

	}
}

