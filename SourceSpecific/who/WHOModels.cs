using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.who
{
	public class WHORecord
	{
		public int id { get; set; }
		public string sd_id { get; set; }
		public string remote_url { get; set; }
		public string display_title { get; set; }
		public string acronym { get; set; }
		public int? study_type_id { get; set; }
		public string study_type { get; set; }
		public string brief_description { get; set; }
		public string study_period { get; set; }
		public string date_prepared { get; set; }
		public DateTime? page_prepared_date { get; set; }
		public string last_updated { get; set; }
		public DateTime? last_revised_date { get; set; }
		public int publication_year { get; set; }
		public string study_website { get; set; }
		public int num_clinical_trial_urls { get; set; }
		public int num_primary_pub_urls { get; set; }
		public int num_associated_papers { get; set; }
		public string resources_available { get; set; }
		public int dataset_consent_type_id { get; set; }
		public string dataset_consent_type { get; set; }
		public string dataset_consent_restrictions { get; set; }

		public List<StudyPrimaryDoc> primary_docs { get; set; }
		public List<RegistryId> registry_ids { get; set; }
		public List<StudyAssocDoc> assoc_docs { get; set; }


		public WHORecord(int _id)
		{
			id = _id;
		}

		public WHORecord()
		{ }

	}



	public class Link
	{
		public string attribute { get; set; }
		public string url { get; set; }

		public Link(string _attribute, string _url)
		{
			attribute = _attribute;
			url = _url;
		}
	}

	public class DataRestrictDetails
	{
		public int? org_id { get; set; }
		public string org_name { get; set; }
	}

	public class ObjectTypeDetails
	{
		public int? type_id { get; set; }
		public string type_name { get; set; }
	}


	public class SponsorDetails
	{
		public int? org_id { get; set; }
		public string org_name { get; set; }
	}

	public class RegistryId
	{
		public string url { get; set; }
		public string nct_id { get; set; }
		public string comment { get; set; }

		public RegistryId(string _url, string _nctid, string _comment)
		{
			url = _url;
			nct_id = _nctid;
			comment = _comment;
		}

		public RegistryId()
		{ }
	}

	public class StudyPrimaryDoc
	{
		public int study_id { get; set; }
		public string sd_id { get; set; }
		public string acronym { get; set; }
		public string url { get; set; }
		public string pubmed_id { get; set; }
		public string comment { get; set; }

		public StudyPrimaryDoc(int _study_id, string _sd_id, string _acronym,
								string _url, string _pubmed_id, string _comment)
		{
			study_id = _study_id;
			sd_id = _sd_id;
			acronym = _acronym;
			url = _url;
			pubmed_id = _pubmed_id;
			comment = _comment;
		}

		public StudyPrimaryDoc()
		{ }
	}

	public class StudyAssocDoc
	{
		public int study_id { get; set; }
		public string sd_id { get; set; }
		public string acronym { get; set; }
		public string link_id { get; set; }
		public string pubmed_id { get; set; }
		public string pmc_id { get; set; }
		public string title { get; set; }
		public string display_title { get; set; }
		public string journal { get; set; }
		public string pub_date { get; set; }

		public StudyAssocDoc(int _study_id, string _sd_id, string _acronym, string _link_id)
		{
			study_id = _study_id;
			sd_id = _sd_id;
			acronym = _acronym;
			link_id = _link_id;
		}

		public StudyAssocDoc()
		{ }
	}

}
