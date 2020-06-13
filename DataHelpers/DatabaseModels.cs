using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
	public class Study
	{
		public string sd_sid { get; set; }
		public string display_title { get; set; }
		public string title_lang_code { get; set; }

		public string brief_description { get; set; }
		public bool bd_contains_html { get; set; }
		public string data_sharing_statement { get; set; }
		public bool dss_contains_html { get; set; }
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

		public DateTime? datetime_of_data_fetch { get; set; }

		public List<StudyIdentifier> identifiers { get; set; }
		public List<StudyTitle> titles { get; set; }
		public List<StudyContributor> contributors { get; set; }
		public List<StudyReference> references { get; set; }
		public List<StudyTopic> topics { get; set; }
		public List<StudyFeature> features { get; set; }
		public List<StudyRelationship> relationships { get; set; }

		public List<DataObject> data_objects { get; set; }
		public List<DataSetProperties> dataset_properties { get; set; }
		public List<DataObjectTitle> object_titles { get; set; }
		public List<DataObjectDate> object_dates { get; set; }
		public List<DataObjectInstance> object_instances { get; set; }

	}

	[Table("sd.studies")]
	public class StudyInDB
	{
		public string sd_sid { get; set; }
		public string display_title { get; set; }
		public string title_lang_code { get; set; }

		public string brief_description { get; set; }
		public bool bd_contains_html { get; set; }
		public string data_sharing_statement { get; set; }
		public bool dss_contains_html { get; set; }
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

		public DateTime? datetime_of_data_fetch { get; set; }

		public StudyInDB(Study s)
		{
			sd_sid = s.sd_sid;
			display_title = s.display_title;
			title_lang_code = s.title_lang_code ?? "en";
			brief_description = s.brief_description;
			bd_contains_html = s.bd_contains_html;
			data_sharing_statement = s.data_sharing_statement;
			dss_contains_html = s.dss_contains_html;
			study_start_year = s.study_start_year;
			study_start_month = s.study_start_month;
			study_type_id = s.study_type_id;
			study_type = s.study_type;
			study_status_id = s.study_status_id;
			study_status = s.study_status;
			study_enrolment = s.study_enrolment;
			study_gender_elig_id = s.study_gender_elig_id;
			study_gender_elig = s.study_gender_elig;
			min_age = s.min_age;
			min_age_units_id = s.min_age_units_id;
			min_age_units = s.min_age_units;
			max_age = s.max_age;
			max_age_units_id = s.max_age_units_id;
			max_age_units = s.max_age_units;
			datetime_of_data_fetch = s.datetime_of_data_fetch;
		}
	}


	public class StudyTitle
	{
		public string sd_sid { get; set; }
		public string title_text { get; set; }
		public int? title_type_id { get; set; }
		public string title_type { get; set; }
		public string title_lang_code { get; set; }
		public int lang_usage_id  { get; set; }
	    public bool is_default { get; set; }
		public string comments { get; set; }

		public StudyTitle(string _sd_sid, string _title_text, int? _title_type_id, string _title_type, bool _is_default)
		{
			sd_sid = _sd_sid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
			is_default = _is_default;
		}

		public StudyTitle(string _sd_sid, string _title_text, int? _title_type_id, string _title_type, bool _is_default, string _comments)
		{
			sd_sid = _sd_sid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
			is_default = _is_default;
			comments = _comments;
		}

		public StudyTitle(string _sd_sid, string _title_text, int? _title_type_id, string _title_type, string _title_lang_code, 
			                   int _lang_usage_id, bool _is_default, string _comments)
		{
			sd_sid = _sd_sid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
			title_lang_code = _title_lang_code;
			lang_usage_id = _lang_usage_id;
			is_default = _is_default;
			comments = _comments;
		}
	}


	public class StudyContributor
	{
		public string sd_sid { get; set; }
		public int? contrib_type_id { get; set; }
		public string contrib_type { get; set; }
		public bool is_individual { get; set; }
		public int? organisation_id { get; set; }
		public string organisation_name { get; set; }
		public int? person_id { get; set; }
		public string person_given_name { get; set; }
		public string person_family_name { get; set; }
		public string person_full_name { get; set; }
		public string person_identifier { get; set; }
		public string identifier_type { get; set; }
		public string person_affiliation { get; set; }
		public string affil_org_id { get; set; }
		public string affil_org_id_type { get; set; }

		public StudyContributor(string _sd_sid, int? _contrib_type_id, string _contrib_type,
								int? _organisation_id, string _organisation_name, string _person_full_name,
								string _person_affiliation)
		{
			sd_sid = _sd_sid;
			contrib_type_id = _contrib_type_id;
			contrib_type = _contrib_type;
			is_individual = (_person_full_name == null) ? false : true;
			organisation_id = _organisation_id;
			organisation_name = _organisation_name;
			person_full_name = _person_full_name;
			person_affiliation = _person_affiliation;
		}

		// more constructors needed here
	}


	public class StudyRelationship
	{
		public string sd_sid { get; set; }
		public int relationship_type_id { get; set; }
		public string relationship_type { get; set; }
		public string target_sd_sid { get; set; }

		public StudyRelationship(string _sd_sid, int _relationship_type_id, string _relationship_type, string _target_sd_sid)
		{
			sd_sid = _sd_sid;
			relationship_type_id = _relationship_type_id;
			relationship_type = _relationship_type;
			target_sd_sid = _target_sd_sid;
		}
	}


	public class StudyReference
	{
		public string sd_sid { get; set; }
		public string pmid { get; set; }
		public string citation { get; set; }
		public string doi { get; set; }
		public string comments { get; set; }

		public StudyReference(string _sd_sid, string _pmid, string _citation, string _doi, string _comments)
		{
			sd_sid = _sd_sid;
			pmid = _pmid;
			citation = _citation;
			doi = _doi;
			comments = _comments;
		}
	}
	

	public class StudyIdentifier
	{
		public string sd_sid { get; set; }
		public string identifier_value { get; set; }
		public int? identifier_type_id { get; set; }
		public string identifier_type { get; set; }
		public int? identifier_org_id { get; set; }
		public string identifier_org { get; set; }
		public string identifier_date { get; set; }
		public string identifier_link { get; set; }

		public StudyIdentifier() { }

		public StudyIdentifier(string _sd_sid, string _identifier_value,
			int? _identifier_type_id, string _identifier_type,
			int? _identifier_org_id, string _identifier_org)
		{
			sd_sid = _sd_sid;
			identifier_value = _identifier_value;
			identifier_type_id = _identifier_type_id;
			identifier_type = _identifier_type;
			identifier_org_id = _identifier_org_id;
			identifier_org = _identifier_org;
		}

		public StudyIdentifier(string _sd_sid, string _identifier_value,
			int? _identifier_type_id, string _identifier_type,
			string _identifier_date, string _identifier_link)
		{
			sd_sid = _sd_sid;
			identifier_value = _identifier_value;
			identifier_type_id = _identifier_type_id;
			identifier_type = _identifier_type;
			identifier_date = _identifier_date;
			identifier_link = _identifier_link;
		}

		public StudyIdentifier(string _sd_sid, string _identifier_value,
			int? _identifier_type_id, string _identifier_type,
			int? _identifier_org_id, string _identifier_org,
			string _identifier_date, string _identifier_link)
		{
			sd_sid = _sd_sid;
			identifier_value = _identifier_value;
			identifier_type_id = _identifier_type_id;
			identifier_type = _identifier_type;
			identifier_org_id = _identifier_org_id;
			identifier_org = _identifier_org;
			identifier_date = _identifier_date;
			identifier_link = _identifier_link;
		}
	}


	public class StudyTopic
	{
		public string sd_sid { get; set; }
		public int topic_type_id { get; set; }
		public string topic_type { get; set; }
		public string topic_value { get; set; }
		public int topic_ct_id { get; set; }
		public string topic_ct { get; set; }
		public string topic_ct_code { get; set; }
		public string where_found { get; set; }

		public StudyTopic(string _sd_sid, int _topic_type_id, string _topic_type,
					 string _topic_value, string _topic_ct_code, string _where_found)
		{
			sd_sid = _sd_sid;
			topic_type_id = _topic_type_id;
			topic_type = _topic_type;
			topic_value = _topic_value;
			topic_ct_code = _topic_ct_code;
			where_found = _where_found;
		}
	}


	public class StudyFeature
	{
		public string sd_sid { get; set; }
		public int? feature_type_id { get; set; }
		public string feature_type { get; set; }
		public int? feature_value_id { get; set; }
		public string feature_value { get; set; }

		public StudyFeature(string _sd_sid, int? _feature_type_id, string _feature_type, int? _feature_value_id, string _feature_value)
		{
			sd_sid = _sd_sid;
			feature_type_id = _feature_type_id;
			feature_type = _feature_type;
			feature_value_id = _feature_value_id;
			feature_value = _feature_value;
		}
	}


	public class DataObject
	{
		public string sd_oid { get; set; }
		public string sd_sid { get; set; }
		public string display_name { get; set; }
		public string doi { get; set; }
		public int doi_status_id { get; set; }
		public int? publication_year { get; set; }
		public int object_class_id { get; set; }
		public string object_class { get; set; }
		public int? object_type_id { get; set; }
		public string object_type { get; set; }
		public int? managing_org_id { get; set; }
		public string managing_org { get; set; }
		public int? access_type_id { get; set; }
		public string access_type { get; set; }
		public string access_details { get; set; }
		public string access_details_url { get; set; }
		public DateTime? url_last_checked { get; set; }
		public bool add_study_contribs { get; set; }
		public bool add_study_topics { get; set; }
		public DateTime? datetime_of_data_fetch { get; set; }

		public DataObject(string _sd_oid, string _sd_sid, string _display_name, int? _publication_year, int _object_class_id,
							string _object_class, int? _object_type_id, string _object_type,
							int? _managing_org_id, string _managing_org, int? _access_type_id, 
							DateTime? _datetime_of_data_fetch)
		{
			sd_oid = _sd_oid;
			sd_sid = _sd_sid;
			display_name = _display_name;
			doi_status_id = 9;
			publication_year = _publication_year;
			object_class_id = _object_class_id;
			object_class = _object_class;
			object_type_id = _object_type_id;
			object_type = _object_type;
			managing_org_id = _managing_org_id;
			managing_org = _managing_org;
			access_type_id = _access_type_id;
			if (_access_type_id == 11) access_type = "Public on-screen access and download";
			if (_access_type_id == 12) access_type = "Public on-screen access (open)";
			add_study_contribs = true;
			add_study_topics = true;
			datetime_of_data_fetch = _datetime_of_data_fetch;
		}

		public DataObject(string _sd_oid, string _sd_sid, string _display_name, int? _publication_year, int _object_class_id,
							string _object_class, int _object_type_id, string _object_type,
							int? _managing_org_id, string _managing_org,
							int? _access_type_id, string _access_type, string _access_details,
							string _access_details_url, DateTime? _url_last_checked,
							DateTime? _datetime_of_data_fetch)
		{
			sd_oid = _sd_oid;
			sd_sid = _sd_sid;
			display_name = _display_name;
			doi_status_id = 9;
			publication_year = _publication_year;
			object_class_id = _object_class_id;
			object_class = _object_class;
			object_type_id = _object_type_id;
			object_type = _object_type;
			managing_org_id = _managing_org_id;
			managing_org = _managing_org;
			access_type_id = _access_type_id;
			access_type = _access_type;
			access_details = _access_details;
			access_details_url = _access_details_url;
			url_last_checked = _url_last_checked;
			add_study_contribs = true;
			add_study_topics = true;
			datetime_of_data_fetch = _datetime_of_data_fetch;
		}

	}


	public class DataSetProperties
	{
		public string sd_oid { get; set; }
		public int? record_keys_type_id { get; set; }
		public string record_keys_type { get; set; }
		public string record_keys_details { get; set; }
		public int? identifiers_type_id { get; set; }
		public string identifiers_type { get; set; }
		public string identifiers_details { get; set; }
		public int? consents_type_id { get; set; }
		public string consents_type { get; set; }
		public string consents_details { get; set; }


		public DataSetProperties(string _sd_oid, 
							int? _record_keys_type_id, string _record_keys_type, string _record_keys_details,
							int? _identifiers_type_id, string _identifiers_type, string _identifiers_details,
							int? _consents_type_id, string _consents_type, string _consents_details)
		{
			sd_oid = _sd_oid;
			record_keys_type_id = _record_keys_type_id;
			record_keys_type = _record_keys_type;
			record_keys_details = _record_keys_details;
			identifiers_type_id = _identifiers_type_id;
			identifiers_type = _identifiers_type;
			identifiers_details = _identifiers_details;
			consents_type_id = _consents_type_id;
			consents_type = _consents_type;
			consents_details = _consents_details;
		}
	}


	public class DataObjectTitle
	{
		public string sd_oid { get; set; }
		public string title_text { get; set; }
		public int? title_type_id { get; set; }
		public string title_type { get; set; }
		public string title_lang_code { get; set; }
		public int lang_usage_id { get; set; }
		public bool is_default { get; set; }
		public string comments { get; set; }

		public DataObjectTitle(string _sd_oid, string _title_text, 
								int _title_type_id, string _title_type, bool _is_default)
		{
			sd_oid = _sd_oid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
    	}

		public DataObjectTitle(string _sd_oid, string _title_text, int? _title_type_id, string _title_type, bool _is_default, string _comments)
		{
			sd_oid = _sd_oid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
			is_default = _is_default;
			comments = _comments;
		}

		public DataObjectTitle(string _sd_oid, string _title_text, int? _title_type_id, string _title_type, string _title_lang_code,
							   int _lang_usage_id, bool _is_default, string _comments)
		{
			sd_oid = _sd_oid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
			title_lang_code = _title_lang_code;
			lang_usage_id = _lang_usage_id;
			is_default = _is_default;
			comments = _comments;
		}
	}


	public class DataObjectInstance
	{
		public string sd_oid { get; set; }
		public int? instance_type_id { get; set; }
		public string instance_type { get; set; }
		public int? repository_org_id { get; set; }
		public string repository_org { get; set; }
		public string url { get; set; }
		public bool url_accessible { get; set; }
		public DateTime? url_last_checked { get; set; }
		public int? resource_type_id { get; set; }
		public string resource_type { get; set; }
		public string resource_size { get; set; }
		public string resource_size_units { get; set; }

		public DataObjectInstance(string _sd_oid, int? _repository_org_id,
					string _repository_org, string _url, bool _url_accessible,
					int? _resource_type_id, string _resource_type)
		{
			sd_oid = _sd_oid;
			repository_org_id = _repository_org_id;
			repository_org = _repository_org;
			url = _url;
			url_accessible = _url_accessible;
			resource_type_id = _resource_type_id;
			resource_type = _resource_type;
		}


		public DataObjectInstance(string _sd_oid, int? _repository_org_id,
					string _repository_org, string _url, bool _url_accessible,
					int? _resource_type_id, string _resource_type, string _resource_size, string _resource_size_units)
		{
			sd_oid = _sd_oid;
			repository_org_id = _repository_org_id;
			repository_org = _repository_org;
			url = _url;
			url_accessible = _url_accessible;
			resource_type_id = _resource_type_id;
			resource_type = _resource_type;
			resource_size = _resource_size;
			resource_size_units = _resource_size_units;
		}


		public DataObjectInstance(string _sd_oid, int? _instance_type_id, string _instance_type, 
			        int? _repository_org_id, string _repository_org, string _url, bool _url_accessible,
					int? _resource_type_id, string _resource_type, string _resource_size, string _resource_size_units)
		{
			sd_oid = _sd_oid;
			instance_type_id = _instance_type_id;
			instance_type = _instance_type;
			repository_org_id = _repository_org_id;
			repository_org = _repository_org;
			url = _url;
			url_accessible = _url_accessible;
			resource_type_id = _resource_type_id;
			resource_type = _resource_type;
			resource_size = _resource_size;
			resource_size_units = _resource_size_units;
		}
	}


	public class DataObjectDate
	{
		public string sd_oid { get; set; }
		public int date_type_id { get; set; }
		public string date_type { get; set; }
		public string date_as_string { get; set; }
		public bool is_date_range { get; set; }
		public int? start_year { get; set; }
		public int? start_month { get; set; }
		public int? start_day { get; set; }
		public int? end_year { get; set; }
		public int? end_month { get; set; }
		public int? end_day { get; set; }
		public string details { get; set; }

		public DataObjectDate(string _sd_oid, int _date_type_id, string _date_type,
									int? _start_year, int? _start_month, int? _start_day, string _date_as_string)
		{
			sd_oid = _sd_oid;
			date_type_id = _date_type_id;
			date_type = _date_type;
			start_year = _start_year;
			start_month = _start_month;
			start_day = _start_day;
			date_as_string = _date_as_string;
		}

		public DataObjectDate(string _sd_oid, int _date_type_id, string _date_type,
			                        string _date_as_string, bool _is_date_range,
									int? _start_year, int? _start_month, int? _start_day,
									int? _end_year, int? _end_month, int? _end_day,
									string _details)
		{
			sd_oid = _sd_oid;
			date_type_id = _date_type_id;
			date_type = _date_type;
			date_as_string = _date_as_string;
			is_date_range = _is_date_range;
			start_year = _start_year;
			start_month = _start_month;
			start_day = _start_day;
			end_year = _end_year;
			end_month = _end_month;
			end_day = _end_day;
			details = _details;
		}
	}


}
