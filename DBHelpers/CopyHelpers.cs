using System;
using System.Collections.Generic;
using System.Text;
using PostgreSQLCopyHelper;

namespace DataHarvester
{
	public static class CopyHelpers
	{
		public static PostgreSQLCopyHelper<StudyIdentifier> study_ids_helper =
			new PostgreSQLCopyHelper<StudyIdentifier>("sd", "study_identifiers")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("identifier_value", x => x.identifier_value)
				.MapInteger("identifier_type_id", x => x.identifier_type_id)
				.MapVarchar("identifier_type", x => x.identifier_type)
				.MapInteger("identifier_org_id", x => x.identifier_org_id)
				.MapVarchar("identifier_org", x => x.identifier_org);


		public static PostgreSQLCopyHelper<StudyTitle> study_titles_helper =
			new PostgreSQLCopyHelper<StudyTitle>("sd", "study_titles")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
				.MapVarchar("title_type", x => x.title_type)
				.MapBoolean("is_default", x => x.is_default)
				.MapVarchar("comments", x => x.comments);


		public static PostgreSQLCopyHelper<StudyRelationship> study_relationship_helper =
			new PostgreSQLCopyHelper<StudyRelationship>("sd", "study_relationships")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("relationship_type_id", x => x.relationship_type_id)
				.MapVarchar("relationship_type", x => x.relationship_type)
				.MapVarchar("target_sd_id", x => x.target_sd_id);


		public static PostgreSQLCopyHelper<StudyReference> study_references_helper =
			new PostgreSQLCopyHelper<StudyReference>("sd", "study_references")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("pmid", x => x.pmid)
				.MapVarchar("citation", x => x.citation)
				.MapVarchar("doi", x => x.doi)
				.MapVarchar("comments", x => x.comments);


		public static PostgreSQLCopyHelper<DataObject> data_objects_helper =
			new PostgreSQLCopyHelper<DataObject>("sd", "data_objects")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
				.MapVarchar("display_title", x => x.display_name)
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
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
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
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
				.MapVarchar("title_type", x => x.title_type)
				.MapBoolean("is_default", x => x.is_default);
		
		
		public static PostgreSQLCopyHelper<DataObjectInstance> object_instances_helper =
			new PostgreSQLCopyHelper<DataObjectInstance>("sd", "object_instances")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
				.MapInteger("repository_org_id", x => x.repository_org_id)
				.MapVarchar("repository_org", x => x.repository_org)
				.MapVarchar("url", x => x.url)
				.MapBoolean("url_accessible", x => x.url_accessible)
				.MapDate("url_last_checked", x => x.url_last_checked)
				.MapInteger("resource_type_id", x => x.resource_type_id)
				.MapVarchar("resource_type", x => x.resource_type);


        public static PostgreSQLCopyHelper<DataObjectDate> object_dates_helper =
			new PostgreSQLCopyHelper<DataObjectDate>("sd", "object_dates")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
				.MapInteger("date_type_id", x => x.date_type_id)
				.MapVarchar("date_type", x => x.date_type)
				.MapInteger("start_year", x => x.start_year)
				.MapInteger("start_month", x => x.start_month)
				.MapInteger("start_day", x => x.start_day)
				.MapVarchar("date_as_string", x => x.date_as_string);
	}
}
