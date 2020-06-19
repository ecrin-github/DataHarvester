using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
	
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

	public class CitationObject
	{
		public string sd_oid { get; set; }
		public int? sd_oid_version { get; set; }
		public string display_title { get; set; }
		public string doi { get; set; }
		public string status { get; set; }
		public string pub_model { get; set; }
		public int? publication_year { get; set; }
		public string publication_status { get; set; }
		public string journal_title { get; set; }
		public string pissn { get; set; }
		public string eissn { get; set; }
		public DateTime? datetime_of_data_fetch { get; set; }

		public List<string> LanguageList { get; set; }
		public List<ObjectTitle> object_titles { get; set; }
		public List<ObjectDescription> object_descriptions { get; set; }
		public List<ObjectLanguage> object_languages { get; set; }
		public List<ObjectContributor> object_contributors { get; set; }
		//public List<Person_Identifier> contrib_identifiers { get; set; }
		//public List<Person_Affiliation> contrib_affiliations { get; set; }
		public List<ObjectInstance> object_instances { get; set; }
		public List<ObjectDBAccessionNumber> accession_numbers { get; set; }
		public List<ObjectTopic> object_topics { get; set; }
		public List<ObjectIdentifier> object_identifiers { get; set; }
		public List<ObjectDate> object_dates { get; set; }
		public List<ObjectPublicationType> publication_types { get; set; }
		public List<ObjectCommentCorrection> comments { get; set; }
	}


	public class ObjectTitle
	{
		public string sd_oid { get; set; }
		public string title_text { get; set; }
		public int? title_type_id { get; set; }
		public string title_type { get; set; }
		public string title_lang_code { get; set; }
		public int lang_usage_id { get; set; }
		public bool is_default { get; set; }
		public string comments { get; set; }

		public ObjectTitle(string _sd_oid, string _title_text, 
								int _title_type_id, string _title_type, bool _is_default)
		{
			sd_oid = _sd_oid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
    	}

		public ObjectTitle(string _sd_oid, string _title_text, int? _title_type_id, string _title_type, bool _is_default, string _comments)
		{
			sd_oid = _sd_oid;
			title_text = _title_text;
			title_type_id = _title_type_id;
			title_type = _title_type;
			is_default = _is_default;
			comments = _comments;
		}

		public ObjectTitle(string _sd_oid, string _title_text, int? _title_type_id, string _title_type, string _title_lang_code,
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


	public class ObjectInstance
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

		public ObjectInstance(string _sd_oid, int? _repository_org_id,
					string _repository_org, string _url, bool _url_accessible,
					int? _resource_type_id, string _resource_type)
		{
			sd_oid = _sd_oid;
			instance_type_id = 1;
			instance_type = "Full Resource";
			repository_org_id = _repository_org_id;
			repository_org = _repository_org;
			url = _url;
			url_accessible = _url_accessible;
			resource_type_id = _resource_type_id;
			resource_type = _resource_type;
		}


		public ObjectInstance(string _sd_oid, int? _repository_org_id,
					string _repository_org, string _url, bool _url_accessible,
					int? _resource_type_id, string _resource_type, 
					string _resource_size, string _resource_size_units)
		{
			sd_oid = _sd_oid;
			instance_type_id = 1;
			instance_type = "Full Resource";
			repository_org_id = _repository_org_id;
			repository_org = _repository_org;
			url = _url;
			url_accessible = _url_accessible;
			resource_type_id = _resource_type_id;
			resource_type = _resource_type;
			resource_size = _resource_size;
			resource_size_units = _resource_size_units;
		}


		public ObjectInstance(string _sd_oid, int? _instance_type_id, string _instance_type, 
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

		public ObjectInstance()
		{ }
	}

	// (Object) Identifier class, a Data Object component

	public class ObjectIdentifier
	{
		public string sd_oid { get; set; }
		public int identifier_type_id { get; set; }
		public string identifier_type { get; set; }
		public string identifier_value { get; set; }
		public int? identifier_org_id { get; set; }
		public string identifier_org { get; set; }
		public string date_applied { get; set; }

		public ObjectIdentifier(string _sd_oid, int _type_id, string _type_name,
				string _id_value, int? _org_id, string _org_name)
		{
			sd_oid = _sd_oid;
			identifier_type_id = _type_id;
			identifier_type = _type_name;
			identifier_value = _id_value;
			identifier_org_id = _org_id;
			identifier_org = _org_name;
		}
	}


	public class ObjectDate
	{
		public string sd_oid { get; set; }
		public int date_type_id { get; set; }
		public string date_type { get; set; }
		public string date_as_string { get; set; }
		public bool date_is_range { get; set; }
		public int? start_year { get; set; }
		public int? start_month { get; set; }
		public int? start_day { get; set; }
		public int? end_year { get; set; }
		public int? end_month { get; set; }
		public int? end_day { get; set; }
		public string details { get; set; }

		public ObjectDate(string _sd_oid, int _date_type_id, string _date_type,
									string _date_as_string, int? _start_year)
		{
			sd_oid = _sd_oid;
			date_type_id = _date_type_id;
			date_type = _date_type;
			date_as_string = _date_as_string;
			start_year = _start_year;
		}

		public ObjectDate(string _sd_oid, int _date_type_id, string _date_type,
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

		public ObjectDate(string _sd_oid, int _date_type_id, string _date_type,
									string _date_as_string, bool _is_date_range,
									int? _start_year, int? _start_month, int? _start_day,
									int? _end_year, int? _end_month, int? _end_day,
									string _details)
		{
			sd_oid = _sd_oid;
			date_type_id = _date_type_id;
			date_type = _date_type;
			date_as_string = _date_as_string;
			date_is_range = _is_date_range;
			start_year = _start_year;
			start_month = _start_month;
			start_day = _start_day;
			end_year = _end_year;
			end_month = _end_month;
			end_day = _end_day;
			details = _details;
		}
	}


	public class ObjectPublicationType
	{
		public string sd_oid { get; set; }
		public string type_name { get; set; }
		
		public ObjectPublicationType(string _sd_oid, string _type_name)
		{
			sd_oid = _sd_oid;
			type_name = _type_name;
		}
	}


	public class ObjectDescription
	{
		public string sd_oid { get; set; }
		public int description_type_id { get; set; }
		public string description_type { get; set; }
		public string label { get; set; }
		public string description_text { get; set; }
		public string lang_code { get; set; }
		public bool? contains_html { get; set; }
	}


	// (Object) Database Accession Number class, a Data Object component

	public class ObjectDBAccessionNumber
	{
		public string sd_oid { get; set; }
		public int bank_id { get; set; }
		public string bank_name { get; set; }
		public string accession_number { get; set; }
	}



	// (Object) Comment Correction class, a Data Object component

	public class ObjectCommentCorrection
	{
		public string sd_oid { get; set; }
		public string ref_type { get; set; }
		public string ref_source { get; set; }
		public string pmid { get; set; }
		public string pmid_version { get; set; }
		public string note { get; set; }
	}


	[Table("sd.object_topics")]
	public class ObjectTopic
	{
		public string sd_oid { get; set; }
		public string topic { get; set; }
		public int topic_type_id { get; set; }
		public string topic_type { get; set; }
		public int? ct_scheme_id { get; set; }
		public string ct_scheme { get; set; }
		public string ct_scheme_code { get; set; }
		public string where_found { get; set; }

	}

	public class ObjectContributor
	{
		public string sd_oid { get; set; }
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
		public string person_ident_srce { get; set; }
		public string person_aff_org { get; set; }
		public string person_aff_org_id { get; set; }
		public string person_aff_org_id_srce { get; set; }

		public ObjectContributor(string _sd_oid, int? _contrib_type_id, string _contrib_type,
								int? _organisation_id, string _organisation_name, string _person_full_name,
								string _person_aff_org)
		{
			sd_oid = _sd_oid;
			contrib_type_id = _contrib_type_id;
			contrib_type = _contrib_type;
			is_individual = (_person_full_name == null) ? false : true;
			organisation_id = _organisation_id;
			organisation_name = _organisation_name;
			person_full_name = _person_full_name;
			person_aff_org = _person_aff_org;
		}

		public ObjectContributor(string _sd_oid, int? _contrib_type_id, string _contrib_type,
								string _person_given_name, string _person_family_name, string _person_full_name,
								string _person_identifier, string _person_ident_srce,
								string _person_aff_org, string _person_aff_org_id, string _person_aff_org_id_srce)
		{
			sd_oid = _sd_oid;
			contrib_type_id = _contrib_type_id;
			contrib_type = _contrib_type;
			is_individual = (_person_full_name == null) ? false : true;
			person_given_name = _person_given_name;
			person_family_name = _person_family_name;
			person_full_name = _person_full_name;
			person_identifier = _person_identifier;
			person_ident_srce = _person_ident_srce;
			person_aff_org = _person_aff_org;
			person_aff_org_id = _person_aff_org_id;
			person_aff_org_id_srce = _person_aff_org_id_srce;
		}
	}

	
	// Person Identifier class, a Data Object component,
	// Stored as part of the contributor record, linked by the person id

	public class PersonIdentifier
	{
		public string sd_oid { get; set; }
		public int person_id { get; set; }
		public string identifier { get; set; }
		public string identifier_source { get; set; }
	}

	// Person affiliation class, a Data Object component,
	// Stored as part of the contributor record, linked by the person id

	public class PersonAffiliation
	{
		public string sd_oid { get; set; }
		public int person_id { get; set; }
		public string affiliation { get; set; }
		public string affil_identifier { get; set; }
		public string affil_ident_source { get; set; }
	}


	// The Object language class, essentially just
	// a string language code attached to the source data Id

	public class ObjectLanguage
	{
		public string sd_oid { get; set; }
		public string lang_code { get; set; }

		public ObjectLanguage(string _sd_oid, string _lang_code)
		{
			sd_oid = _sd_oid;
			lang_code = _lang_code;
		}
	}

	// The class used to store data in the Data_objects table -
	// essentially the Data Object without its repeating components.

	[Table("sd.Data_objects")]
	public class CitationObject_in_DB
	{
		[ExplicitKey]
		public string sd_oid { get; set; }
		public int? sd_id_version { get; set; }
		public string display_title { get; set; }
		public string doi { get; set; }
		public string status { get; set; }
		public string pub_model { get; set; }
		public int? publication_year { get; set; }
		public string publication_status { get; set; }
		public string journal_title { get; set; }
		public string pissn { get; set; }
		public string eissn { get; set; }
		public DateTime? datetime_of_data_fetch { get; set; }
	}

}
