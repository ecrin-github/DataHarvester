using System;
using System.Collections.Generic;
using System.Text;
using Dapper.Contrib.Extensions;
using PostgreSQLCopyHelper;

namespace DataHarvester.ctg
{
	/*
	[Table("studies")]
	public class StudyInDB
	{

		[ExplicitKey]
		public string sd_id { get; set; }

		public string display_title { get; set; }
		public string brief_description { get; set; }
		public string data_sharing_statement { get; set; }
		public int? study_start_year { get; set; }
		public int? study_start_month { get; set; }

		public int? study_type_id { get; set; }
		public string study_type { get; set; }
		public int? study_status_id { get; set; }
		public string study_status { get; set; }
		public int? study_enrolment { get; set; }
		public int? study_gender_elig_id { get; set; }
		public string study_gender_elig { get; set; }

		public int? min_age { get; set; }
		public int? min_age_units_id { get; set; }
		public string min_age_units { get; set; }
		public int? max_age { get; set; }
		public int? max_age_units_id { get; set; }
		public string max_age_units { get; set; }

		public StudyInDB(string _sd_id, string _display_title, int? _study_type_id, string _study_type,
						 int? _study_status_id, string _study_status, string _brief_description,
						 string _data_sharing_statement, int? _study_start_year, int? _study_start_month)
		{
			sd_id = _sd_id;
			display_title = _display_title;
			study_type_id = _study_type_id;
			study_type = _study_type;
			study_status_id = _study_status_id;
			study_status = _study_status;
			brief_description = _brief_description;
			data_sharing_statement = _data_sharing_statement;
			study_start_year = _study_start_year;
			study_start_month = _study_start_month;
		}
	}
	*/

	/*
	public class Mappings
	{
		public PostgreSQLCopyHelper<Title> title_copyhelper =	
			new PostgreSQLCopyHelper<Title>("sd", "study_titles")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
				.MapVarchar("title_type", x => x.title_type)
				.MapBoolean("is_default", x => x.is_default);

		public PostgreSQLCopyHelper<Identifier> identifier_copyhelper =
			new PostgreSQLCopyHelper<Identifier>("sd", "study_identifiers")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("identifier_value", x => x.identifier_value)
				.MapInteger("identifier_type_id", x => x.identifier_type_id)
				.MapVarchar("identifier_type", x => x.identifier_type)
				.MapInteger("identifier_org_id", x => x.identifier_org_id)
				.MapVarchar("identifier_org", x => x.identifier_org)
				.MapVarchar("identifier_date", x => x.identifier_date)
				.MapVarchar("identifier_link", x => x.identifier_link);

		public PostgreSQLCopyHelper<Contributor> contributor_copyhelper =
			new PostgreSQLCopyHelper<Contributor>("sd", "study_contributors")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("contrib_type_id", x => x.contrib_type_id)
				.MapVarchar("contrib_type", x => x.contrib_type)
				.MapBoolean("is_individual", x => x.is_individual)
				.MapVarchar("org_name", x => x.org_name)
				.MapVarchar("person_name", x => x.person_name)
				.MapVarchar("person_affiliation", x => x.person_affiliation)
				.MapVarchar("source_field", x => x.source_field);

		public PostgreSQLCopyHelper<StudyRelationship> relationship_copyhelper =
			new PostgreSQLCopyHelper<StudyRelationship>("sd", "study_relationships")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("relationship_type_id", x => x.relationship_type_id)
				.MapVarchar("relationship_type", x => x.relationship_type)
				.MapVarchar("target_sd_id", x => x.target_sd_id);

		public PostgreSQLCopyHelper<Reference> reference_copyhelper =
			new PostgreSQLCopyHelper<Reference>("sd", "study_references")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("pmid", x => x.pmid)
				.MapVarchar("citation", x => x.citation)
				.MapVarchar("retraction", x => x.retraction);

		public PostgreSQLCopyHelper<SeeAlsoLink> link_copyhelper =
			new PostgreSQLCopyHelper<SeeAlsoLink>("sd", "study_links")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("link_label", x => x.link_label)
				.MapVarchar("link_url", x => x.link_url);

		public PostgreSQLCopyHelper<AvailableIPD> ipd_copyhelper =
			new PostgreSQLCopyHelper<AvailableIPD>("sd", "study_ipd_available")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("ipd_id", x => x.ipd_id)
				.MapVarchar("ipd_type", x => x.ipd_type)
				.MapVarchar("ipd_url", x => x.ipd_url)
				.MapVarchar("ipd_comment", x => x.ipd_comment);

		public PostgreSQLCopyHelper<Topic> topic_copyhelper =
			new PostgreSQLCopyHelper<Topic>("sd", "study_topics")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapVarchar("topic_type", x => x.topic_type)
				.MapVarchar("topic", x => x.topic)
				.MapVarchar("mesh_code", x => x.mesh_code)
				.MapVarchar("where_found", x => x.where_found);

		public PostgreSQLCopyHelper<Feature> feature_copyhelper =
			new PostgreSQLCopyHelper<Feature>("sd", "study_features")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("feature_type_id", x => x.feature_type_id)
				.MapVarchar("feature_type", x => x.feature_type)
				.MapInteger("feature_value_id", x => x.feature_value_id)
				.MapVarchar("feature_value", x => x.feature_value);

		public PostgreSQLCopyHelper<DataObject> data_object_copyhelper =
			new PostgreSQLCopyHelper<DataObject>("sd", "data_objects")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
				.MapVarchar("display_title", x => x.display_title)
				.MapInteger("doi_status_id", x => x.doi_status_id)
				.MapInteger("publication_year", x => x.publication_year)
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
				.MapBoolean("add_study_contribs", x => x.add_study_contribs);

		public PostgreSQLCopyHelper<DataObjectTitle> object_title_copyhelper =
			new PostgreSQLCopyHelper<DataObjectTitle>("sd", "object_titles")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
				.MapVarchar("title_type", x => x.title_type)
				.MapBoolean("is_default", x => x.is_default);

		public PostgreSQLCopyHelper<DataObjectDate> object_date_copyhelper =
			new PostgreSQLCopyHelper<DataObjectDate>("sd", "object_dates")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
				.MapInteger("date_type_id", x => x.date_type_id)
				.MapVarchar("date_type", x => x.date_type)
				.MapInteger("start_year", x => x.start_year)
				.MapInteger("start_month", x => x.start_month)
				.MapInteger("start_day", x => x.start_day)
				.MapVarchar("date_as_string", x => x.date_as_string);

		public PostgreSQLCopyHelper<DataObjectInstance> object_instance_copyhelper =
			new PostgreSQLCopyHelper<DataObjectInstance>("sd", "object_instances")
				.MapVarchar("sd_id", x => x.sd_id)
				.MapInteger("do_id", x => x.do_id)
			    .MapInteger("instance_type_id", x => x.instance_type_id)
			    .MapVarchar("instance_type", x => x.instance_type)
				.MapInteger("repository_org_id", x => x.repository_org_id)
				.MapVarchar("repository_org", x => x.repository_org)
				.MapVarchar("url", x => x.url)
				.MapBoolean("url_accessible", x => x.url_accessible)
				.MapInteger("resource_type_id", x => x.resource_type_id)
				.MapVarchar("resource_type", x => x.resource_type);

	}
	*/

}
