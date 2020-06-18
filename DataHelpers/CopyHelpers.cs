using System;
using System.Collections.Generic;
using System.Text;
using PostgreSQLCopyHelper;

namespace DataHarvester
{
	public static class StudyCopyHelpers
	{
		public static PostgreSQLCopyHelper<StudyIdentifier> study_ids_helper =
			new PostgreSQLCopyHelper<StudyIdentifier>("sd", "study_identifiers")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapVarchar("identifier_value", x => x.identifier_value)
				.MapInteger("identifier_type_id", x => x.identifier_type_id)
				.MapVarchar("identifier_type", x => x.identifier_type)
				.MapInteger("identifier_org_id", x => x.identifier_org_id)
				.MapVarchar("identifier_org", x => x.identifier_org);

		public static PostgreSQLCopyHelper<StudyTitle> study_titles_helper =
			new PostgreSQLCopyHelper<StudyTitle>("sd", "study_titles")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
				.MapVarchar("title_type", x => x.title_type)
				.MapBoolean("is_default", x => x.is_default)
				.MapVarchar("comments", x => x.comments);


		public static PostgreSQLCopyHelper<StudyTopic> study_topics_helper =
			new PostgreSQLCopyHelper<StudyTopic>("sd", "study_topics")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapInteger("topic_type_id", x => x.topic_type_id)
				.MapVarchar("topic_type", x => x.topic_type)
				.MapVarchar("topic_value", x => x.topic_value)
				.MapInteger("topic_ct_id", x => x.topic_ct_id)
				.MapVarchar("topic_ct", x => x.topic_ct)
				.MapVarchar("topic_ct_code", x => x.topic_ct_code)
				.MapVarchar("where_found", x => x.where_found);


		public static PostgreSQLCopyHelper<StudyContributor> study_contributors_helper =
			new PostgreSQLCopyHelper<StudyContributor>("sd", "study_contributors")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapInteger("contrib_type_id", x => x.contrib_type_id)
				.MapVarchar("contrib_type", x => x.contrib_type)
				.MapBoolean("is_individual", x => x.is_individual)
				.MapInteger("organisation_id", x => x.organisation_id)
				.MapVarchar("organisation_name", x => x.organisation_name)
				.MapInteger("person_id", x => x.person_id)
				.MapVarchar("person_given_name", x => x.person_given_name)
				.MapVarchar("person_family_name", x => x.person_family_name)
				.MapVarchar("person_full_name", x => x.person_full_name)
				.MapVarchar("person_identifier", x => x.person_identifier)
				.MapVarchar("identifier_type", x => x.identifier_type)
				.MapVarchar("person_affiliation", x => x.person_affiliation)
				.MapVarchar("affil_org_id", x => x.affil_org_id)
				.MapVarchar("affil_org_id_type", x => x.affil_org_id_type);


		public static PostgreSQLCopyHelper<StudyRelationship> study_relationship_helper =
			new PostgreSQLCopyHelper<StudyRelationship>("sd", "study_relationships")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapInteger("relationship_type_id", x => x.relationship_type_id)
				.MapVarchar("relationship_type", x => x.relationship_type)
				.MapVarchar("target_sd_sid", x => x.target_sd_sid);


		public static PostgreSQLCopyHelper<StudyLink> study_links_helper =
			new PostgreSQLCopyHelper<StudyLink>("sd", "study_links")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapVarchar("link_label", x => x.link_label)
				.MapVarchar("link_url", x => x.link_url);


		public static PostgreSQLCopyHelper<StudyFeature> study_features_helper =
			new PostgreSQLCopyHelper<StudyFeature>("sd", "study_features")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapInteger("feature_type_id", x => x.feature_type_id)
				.MapVarchar("feature_type", x => x.feature_type)
				.MapInteger("feature_value_id", x => x.feature_value_id)
				.MapVarchar("feature_value", x => x.feature_value);


		public static PostgreSQLCopyHelper<StudyReference> study_references_helper =
			new PostgreSQLCopyHelper<StudyReference>("sd", "study_references")
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapVarchar("pmid", x => x.pmid)
				.MapVarchar("citation", x => x.citation)
				.MapVarchar("doi", x => x.doi)
				.MapVarchar("comments", x => x.comments);


		public static PostgreSQLCopyHelper<AvailableIPD> study_ipd_copyhelper =
			new PostgreSQLCopyHelper<AvailableIPD>("sd", "study_ipd_available")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("ipd_id", x => x.ipd_id)
				.MapVarchar("ipd_type", x => x.ipd_type)
				.MapVarchar("ipd_url", x => x.ipd_url)
				.MapVarchar("ipd_comment", x => x.ipd_comment);

	}


	public static class ObjectCopyHelpers
	{
		public static PostgreSQLCopyHelper<DataObject> data_objects_helper =
			new PostgreSQLCopyHelper<DataObject>("sd", "data_objects")
				.MapVarchar("sd_oid", x => x.sd_oid)
				.MapVarchar("sd_sid", x => x.sd_sid)
				.MapVarchar("display_title", x => x.display_name)
			    .MapVarchar("doi", x => x.doi)
				.MapInteger("doi_status_id", x => x.doi_status_id)
				.MapInteger("publication_year ", x => x.publication_year)
				.MapInteger("object_class_id", x => x.object_class_id)
				.MapVarchar("object_class", x => x.object_class)
				.MapInteger("object_type_id", x => x.object_type_id)
				.MapVarchar("object_type", x => x.object_type)
				.MapInteger("managing_org_id", x => x.managing_org_id)
				.MapVarchar("managing_org", x => x.managing_org)
				.MapInteger("access_type_id", x => x.access_type_id)
				.MapVarchar("access_type", x => x.access_type)
				.MapVarchar("access_details", x => x.access_details)
				.MapVarchar("access_details_url", x => x.access_details_url)
				.MapDate("url_last_checked", x => x.url_last_checked)
				.MapBoolean("add_study_contribs", x => x.add_study_contribs)
				.MapBoolean("add_study_topics", x => x.add_study_topics)
			    .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);


		public static PostgreSQLCopyHelper<DataSetProperties> dataset_properties_helper =
			new PostgreSQLCopyHelper<DataSetProperties>("sd", "dataset_properties")
				.MapVarchar("sd_oid", x => x.sd_oid)
				.MapInteger("record_keys_type_id", x => x.record_keys_type_id)
				.MapVarchar("record_keys_type", x => x.record_keys_type)
				.MapVarchar("record_keys_details", x => x.record_keys_details)
				.MapInteger("identifiers_type_id", x => x.identifiers_type_id)
				.MapVarchar("identifiers_type", x => x.identifiers_type)
				.MapVarchar("identifiers_details", x => x.identifiers_details)
				.MapInteger("consents_type_id", x => x.consents_type_id)
				.MapVarchar("consents_type", x => x.consents_type)
				.MapVarchar("consents_details", x => x.consents_details);


		public static PostgreSQLCopyHelper<DataObjectTitle> object_titles_helper =
			new PostgreSQLCopyHelper<DataObjectTitle>("sd", "object_titles")
				.MapVarchar("sd_oid", x => x.sd_oid)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
				.MapVarchar("title_type", x => x.title_type)
				.MapBoolean("is_default", x => x.is_default);
		
		
		public static PostgreSQLCopyHelper<DataObjectInstance> object_instances_helper =
			new PostgreSQLCopyHelper<DataObjectInstance>("sd", "object_instances")
				.MapVarchar("sd_oid", x => x.sd_oid)
				.MapInteger("repository_org_id", x => x.repository_org_id)
				.MapVarchar("repository_org", x => x.repository_org)
				.MapVarchar("url", x => x.url)
				.MapBoolean("url_accessible", x => x.url_accessible)
				.MapDate("url_last_checked", x => x.url_last_checked)
				.MapInteger("resource_type_id", x => x.resource_type_id)
				.MapVarchar("resource_type", x => x.resource_type);


        public static PostgreSQLCopyHelper<DataObjectDate> object_dates_helper =
			new PostgreSQLCopyHelper<DataObjectDate>("sd", "object_dates")
				.MapVarchar("sd_oid", x => x.sd_oid)
				.MapInteger("date_type_id", x => x.date_type_id)
				.MapVarchar("date_type", x => x.date_type)
				.MapInteger("start_year", x => x.start_year)
				.MapInteger("start_month", x => x.start_month)
				.MapInteger("start_day", x => x.start_day)
				.MapVarchar("date_as_string", x => x.date_as_string);


		public static PostgreSQLCopyHelper<ObjectContributor> contributor_copyhelper =
			new PostgreSQLCopyHelper<ObjectContributor>("sd", "object_contributors")
				.MapInteger("sd_id", x => x.sd_id)
				.MapInteger("person_id", x => x.person_id)
				.MapInteger("contributor_type_id", x => x.contributor_type_id)
				.MapVarchar("contributor_type", x => x.contributor_type)
				.MapVarchar("family_name", x => x.family_name)
				.MapVarchar("given_name", x => x.given_name)
				.MapVarchar("suffix", x => x.suffix)
				.MapVarchar("initials", x => x.initials)
				.MapVarchar("collective_name", x => x.collective_name);

		/*
		public PostgreSQLCopyHelper<Person_Identifier> persid_copyhelper =
			new PostgreSQLCopyHelper<Person_Identifier>("sd", "people_identifiers")
				.MapInteger("sd_id", x => x.sd_id)
				.MapInteger("person_id", x => x.person_id)
				.MapVarchar("identifier", x => x.identifier)
				.MapVarchar("identifier_source", x => x.identifier_source);


		public PostgreSQLCopyHelper<Person_Affiliation> persaff_copyhelper =
			new PostgreSQLCopyHelper<Person_Affiliation>("sd", "people_affiliations")
				.MapInteger("sd_id", x => x.sd_id)
				.MapInteger("person_id", x => x.person_id)
				.MapVarchar("affiliation", x => x.affiliation)
				.MapVarchar("affil_identifier", x => x.affil_identifier)
				.MapVarchar("affil_ident_source", x => x.affil_ident_source);
		*/


		public static PostgreSQLCopyHelper<DataObjectIdentifier> identifier_copyhelper =
			new PostgreSQLCopyHelper<DataObjectIdentifier>("sd", "object_identifiers")
				.MapInteger("sd_id", x => x.sd_id)
				.MapInteger("identifier_type_id", x => x.identifier_type_id)
				.MapVarchar("identifier_type", x => x.identifier_type)
				.MapVarchar("identifier_value", x => x.identifier_value)
				.MapInteger("identifier_org_id", x => x.identifier_org_id)
				.MapVarchar("identifier_org", x => x.identifier_org)
				.MapVarchar("date_applied", x => x.date_applied);


		public static PostgreSQLCopyHelper<DataObjectDescription> description_copyhelper =
			new PostgreSQLCopyHelper<DataObjectDescription>("sd", "object_descriptions")
				.MapInteger("sd_id", x => x.sd_id)
				.MapInteger("description_type_id", x => x.description_type_id)
				.MapVarchar("description_type", x => x.description_type)
				.MapVarchar("label", x => x.label)
				.MapVarchar("description_text", x => x.description_text)
				.MapVarchar("lang_code", x => x.lang_code)
				.MapBoolean("contains_html", x => x.contains_html);


		public static PostgreSQLCopyHelper<DB_Accession_Number> db_acc_number_copyhelper =
			new PostgreSQLCopyHelper<DB_Accession_Number>("sd", "object_links")
				.MapInteger("sd_id", x => x.sd_id)
				.MapInteger("bank_id", x => x.bank_id)
				.MapVarchar("bank_name", x => x.bank_name)
				.MapVarchar("accession_number", x => x.accession_number);


		public static PostgreSQLCopyHelper<DataObjectPublication_Type> pub_type_copyhelper =
			new PostgreSQLCopyHelper<DataObjectPublication_Type>("sd", "object_public_types")
				.MapInteger("sd_id", x => x.sd_id)
				.MapVarchar("type_name", x => x.type_name);


		public static PostgreSQLCopyHelper<DataObjectCommentCorrection> comment_correction_copyhelper =
			new PostgreSQLCopyHelper<DataObjectCommentCorrection>("sd", "object_corrections")
				.MapInteger("sd_id", x => x.sd_id)
				.MapVarchar("ref_type", x => x.ref_type)
				.MapVarchar("ref_source", x => x.ref_source)
				.MapVarchar("pmid", x => x.pmid)
				.MapVarchar("pmid_version", x => x.pmid_version)
				.MapVarchar("note", x => x.note);


		public static PostgreSQLCopyHelper<ObjectTopic> keyword_copyhelper =
			new PostgreSQLCopyHelper<ObjectTopic>("sd", "object_topics")
				.MapInteger("sd_id", x => x.sd_id)
				.MapVarchar("topic", x => x.topic)
				.MapInteger("topic_type_id", x => x.topic_type_id)
				.MapVarchar("topic_type", x => x.topic_type)
				.MapInteger("ct_scheme_id", x => x.ct_scheme_id)
				.MapVarchar("ct_scheme", x => x.ct_scheme)
				.MapVarchar("ct_scheme_code", x => x.ct_scheme_code)
				.MapVarchar("where_found", x => x.where_found);


		public static PostgreSQLCopyHelper<ObjectLanguage> object_language_copyhelper =
			new PostgreSQLCopyHelper<ObjectLanguage>("sd", "object_languages")
				.MapInteger("sd_id", x => x.sd_id)
				.MapVarchar("lang_code", x => x.lang_code);

	}
}
