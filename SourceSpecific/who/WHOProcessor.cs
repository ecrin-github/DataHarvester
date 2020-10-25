using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace DataHarvester.who
{
    public class WHOProcessor
	{

		public Study ProcessData(WHORecord st, DateTime? download_datetime, DataLayer common_repo, WHODataLayer who_repo)
		{
			Study s = new Study();

			// get date retrieved in object fetch
			// transfer to study and data object records

			List<StudyIdentifier> study_identifiers = new List<StudyIdentifier>();
			List<StudyTitle> study_titles = new List<StudyTitle>();
			List<DataHarvester.StudyFeature> study_features = new List<DataHarvester.StudyFeature>();
			List<StudyTopic> study_topics = new List<StudyTopic>();
			List<StudyContributor> study_contributors = new List<StudyContributor>();

			List<DataObject> data_objects = new List<DataObject>();
			List<ObjectTitle> data_object_titles = new List<ObjectTitle>();
			List<ObjectDate> data_object_dates = new List<ObjectDate>();
			List<ObjectInstance> data_object_instances = new List<ObjectInstance>();

			// transfer features of main study object
			// In most cases study will have already been registered in CGT

			string sid = st.sd_sid;
			s.sd_sid = sid;
			s.datetime_of_data_fetch = download_datetime;

			SplitDate registration_date = null;
			if (!string.IsNullOrEmpty(st.date_registration))
			{
				registration_date = DateHelpers.GetDatePartsFromISOString(st.date_registration);
			}

			study_identifiers.Add(new StudyIdentifier(sid, sid, 11, "Trial Registry ID", st.source_id,
									 get_source_name(st.source_id), registration_date?.date_string, null));

			// titles
			string public_title = StringHelpers.CheckTitle(st.public_title);
			string scientific_title = StringHelpers.CheckTitle(st.scientific_title);

			if (public_title == "")
			{
				if (scientific_title != "")
				{
					if (scientific_title.Length < 11)
					{
						study_titles.Add(new StudyTitle(sid, scientific_title, 14, "Acronym or Abbreviation", true));
					}
					else
					{
						study_titles.Add(new StudyTitle(sid, scientific_title, 16, "Trial registry title", true));
					}
					s.display_title = scientific_title;
				}
				else
				{
					s.display_title = "No public or scientific title provided";
				}
			}
			else
			{
				if (public_title.Length < 11)
				{
					study_titles.Add(new StudyTitle(sid, public_title, 14, "Acronym or Abbreviation", true));
				}
				else
				{
					study_titles.Add(new StudyTitle(sid, public_title, 15, "Public Title", true));
				}
				s.display_title = public_title;

				if (scientific_title != "" && scientific_title.ToLower() != public_title.ToLower())
				{
					study_titles.Add(new StudyTitle(sid, scientific_title, 16, "Trial registry title", false));
				}
			}

			s.title_lang_code = "en";  // as a default

			// need a mechanism, here to try and identify at least majot language variations
			// e.g. Spanish, German, French - may be linkable to the source registry

			// brief description
			string interventions = "", primary_outcome = "", study_design = "";

			if (!string.IsNullOrEmpty(st.interventions))
			{
				interventions = st.interventions.Trim();
				if (!interventions.ToLower().StartsWith("intervention"))
				{
					interventions = "Interventions: " + interventions;
				}
				if (!interventions.EndsWith(".") && !interventions.EndsWith(";"))
				{
					interventions += ".";
				}
			}

			if (!string.IsNullOrEmpty(st.primary_outcome))
			{
				primary_outcome = st.primary_outcome.Trim();
				if (!primary_outcome.ToLower().StartsWith("primary"))
				{
					primary_outcome = "Primary outcome(s): " + primary_outcome;
				}

				if (!primary_outcome.EndsWith(".") && !primary_outcome.EndsWith(";")
					&& !primary_outcome.EndsWith("?"))
				{
					primary_outcome += ".";
				}
			}


			if (!string.IsNullOrEmpty(st.design_string)
				&& !st.design_string.ToLower().Contains("not selected"))
			{
				study_design = st.design_string.Trim();
				if (!study_design.ToLower().StartsWith("primary"))
				{
					study_design = "Study Design: " + study_design;
				}

				if (!study_design.EndsWith(".") && !study_design.EndsWith(";"))
				{
					study_design += ".";
				}
			}

			s.brief_description = (interventions + " " + primary_outcome + " " + study_design).Trim();
			if (s.brief_description.Contains("<"))
			{
				s.brief_description = HtmlHelpers.replace_tags(s.brief_description);
				s.bd_contains_html = HtmlHelpers.check_for_tags(s.brief_description);
			}

			// data sharing statement
			if (!string.IsNullOrEmpty(st.ipd_description)
				&& st.ipd_description.Length > 10
				&& st.ipd_description.ToLower() != "not available"
				&& st.ipd_description.ToLower() != "not avavilable"
				&& st.ipd_description.ToLower() != "not applicable"
				&& !st.ipd_description.Contains("justification or reason for"))
			{
				s.data_sharing_statement = st.ipd_description;
				if (s.data_sharing_statement.Contains("<"))
				{
					s.data_sharing_statement = HtmlHelpers.replace_tags(s.data_sharing_statement);
					s.dss_contains_html = HtmlHelpers.check_for_tags(s.data_sharing_statement);
				}
			}

			if (!string.IsNullOrEmpty(st.date_enrolment))
			{
				SplitDate enrolment_date = DateHelpers.GetDatePartsFromISOString(st.date_enrolment);
				if (enrolment_date?.year > 1960)
				{
					s.study_start_year = enrolment_date.year;
					s.study_start_month = enrolment_date.month;
				}
			}

			// study type and status 
			if (!string.IsNullOrEmpty(st.study_type))
			{
				if (st.study_type.StartsWith("Other"))
				{
					s.study_type = "Other";
					s.study_type_id = 16;
				}
				else
				{
					s.study_type = st.study_type; ;
					s.study_type_id = TypeHelpers.GetTypeId(s.study_type);
				}
			}

			if (!string.IsNullOrEmpty(st.study_status))
			{
				if (st.study_status.StartsWith("Other"))
				{
					s.study_status = "Other";
					s.study_status_id = 24;

				}
				else
				{
					s.study_status = st.study_status;
					s.study_status_id = TypeHelpers.GetStatusId(s.study_status);
				}
			}


			// enrolment targets, gender and age groups
			int? enrolment = 0;

			// use actual enrolment figure if present and not a data or a dummy figure
			if (!string.IsNullOrEmpty(st.results_actual_enrollment)
				&& !st.results_actual_enrollment.Contains("9999")
				&& !Regex.Match(st.results_actual_enrollment, @"\d{4}-\d{2}-\d{2}").Success)
			{
				if (Regex.Match(st.results_actual_enrollment, @"\d+").Success)
				{
					int numeric_value = Int32.Parse(Regex.Match(st.results_actual_enrollment, @"\d+").Value);
					if (numeric_value < 10000)
					{
						enrolment = numeric_value;
					}
				}
			}

			// use the target if that is all that is available
			if (enrolment == 0 && !string.IsNullOrEmpty(st.target_size)
				&& !st.target_size.Contains("9999"))
			{
				if (Regex.Match(st.target_size, @"\d+").Success)
				{
					int numeric_value = Int32.Parse(Regex.Match(st.target_size, @"\d+").Value);
					if (numeric_value < 10000)
					{
						enrolment = numeric_value;
					}
				}
			}
			s.study_enrolment = enrolment > 0 ? enrolment : null;


			if (Int32.TryParse(st.agemin, out int min))
			{
				s.min_age = min;
				if (st.agemin_units.StartsWith("Other"))
				{
					// was not classified previously...
					s.min_age_units = TypeHelpers.GetTimeUnits(st.agemin_units);
				}
				else
				{
					s.min_age_units = st.agemin_units;
				}
				if (s.min_age_units != null)
				{
					s.min_age_units_id = TypeHelpers.GetTimeUnitsId(s.min_age_units);
				}
			}


			if (Int32.TryParse(st.agemax, out int max))
			{
				if (max != 0)
				{
					s.max_age = max;
					if (st.agemax_units.StartsWith("Other"))
					{
						// was not classified previously...
						s.max_age_units = TypeHelpers.GetTimeUnits(st.agemax_units);
					}
					else
					{
						s.max_age_units = st.agemax_units;
					}
					if (s.max_age_units != null)
					{
						s.max_age_units_id = TypeHelpers.GetTimeUnitsId(s.max_age_units);
					}
				}
			}


			if (st.gender.Contains("?? Unable to classify"))
            {
				st.gender = "Not provided";
			}

            s.study_gender_elig = st.gender;
			s.study_gender_elig_id = TypeHelpers.GetGenderEligId(st.gender);


			// Add study attribute records.

			// study contributors - Sponsor N.B. default below
			string sponsor_name = "No organisation name provided in source data";

			if (!string.IsNullOrEmpty(st.primary_sponsor))
			{
				sponsor_name = StringHelpers.TidyOrgName(st.primary_sponsor, sid);
				string sponsor = sponsor_name.ToLower();
				if (StringHelpers.FilterOut_Null_OrgNames(sponsor) != "")
				{
					if (sponsor.StartsWith("dr ") || sponsor.StartsWith("dr. ")
						|| sponsor.StartsWith("prof ") || sponsor.StartsWith("prof. ")
						|| sponsor.StartsWith("professor "))
					{
						study_contributors.Add(new StudyContributor(sid, 54, "Trial Sponsor", null, null, sponsor_name, null));
					}
					else
					{
						study_contributors.Add(new StudyContributor(sid, 54, "Trial Sponsor", null, sponsor_name, null, null));
					}
				}
			}

			// Study lead
			string study_lead = "";
			if (!string.IsNullOrEmpty(st.scientific_contact_givenname) || !string.IsNullOrEmpty(st.scientific_contact_familyname))
			{
				string givenname = st.scientific_contact_givenname ?? "";
				string familyname = st.scientific_contact_familyname ?? "";
				string full_name = (givenname + " " + familyname).Trim();
				study_lead = full_name;  // for later comparison
				string affiliation = st.scientific_contact_affiliation ?? "";
				study_contributors.Add(new StudyContributor(sid, 51, "Study Lead", null, null, full_name, affiliation));
			}

			// public contact
			if (!string.IsNullOrEmpty(st.public_contact_givenname) || !string.IsNullOrEmpty(st.public_contact_familyname))
			{
				string givenname = st.public_contact_givenname ?? "";
				string familyname = st.public_contact_familyname ?? "";
				string full_name = (givenname + " " + familyname).Trim();
				if (full_name != study_lead)  // often duplicated
				{
					string affiliation = st.public_contact_affiliation ?? "";
					study_contributors.Add(new StudyContributor(sid, 56, "Public Contact", null, null, full_name, affiliation));
				}
			}

			// study features 
			if (st.study_features.Count > 0)
			{
				foreach (StudyFeature f in st.study_features)
				{
					study_features.Add(new DataHarvester.StudyFeature(sid, f.ftype_id, f.ftype, f.fvalue_id, f.fvalue));
				}
			}

			//study identifiers
			if (st.secondary_ids.Count > 0)
			{
				foreach (Secondary_Id id in st.secondary_ids)
				{
					if (id.sec_id_source == null)
					{
						study_identifiers.Add(new StudyIdentifier(sid, id.processed_id, 14, "Sponsor ID", null, sponsor_name));
					}
					else
					{
						if (id.sec_id_source == 102000)
						{
							study_identifiers.Add(new StudyIdentifier(sid, id.processed_id, 41, "Regulatory Body ID", 102000, "Anvisa (Brazil)"));
						}
						else if (id.sec_id_source == 102001)
						{
							study_identifiers.Add(new StudyIdentifier(sid, id.processed_id, 12, "Ethics Review ID", 102001, "Comitê de Ética em Pesquisa (local) (Brazil)"));
						}
						else
						{
							study_identifiers.Add(new StudyIdentifier(sid, id.processed_id, 11, "Trial Registry ID", id.sec_id_source, get_source_name(id.sec_id_source)));
						}
					}
				}
			}

			// study conditions
			if (st.condition_list.Count > 0)
			{
				foreach (StudyCondition sc in st.condition_list)
				{
					if (!string.IsNullOrEmpty(sc.condition))
					{
						char[] chars_to_trim = {' ', '?', ':', '*', '/', '-', '_', '+', '='};
						string cond = sc.condition.Trim(chars_to_trim);
						if (!string.IsNullOrEmpty(cond) && cond.ToLower() != "not applicable" && cond.ToLower() != "&quot")
						{
							if (sc.code == null)
							{
								study_topics.Add(new StudyTopic(sid, 13, "Condition", cond));
							}
							else
							{
								if (sc.code_system == "ICD 10")
								{
									study_topics.Add(new StudyTopic(sid, 13, "Condition", cond, 12, sc.code, ""));
								}
							}
						}
					}
				}
			}


			// Create data object records.
			// registry entry
			string name_base = s.display_title;
			string object_display_title = name_base + " :: " + "Registry web page";
			string sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

			int? pub_year = registration_date?.year;

			string source_name = get_source_name(st.source_id);
			data_objects.Add(new DataObject(sd_oid, sid, object_display_title, pub_year, 23, "Text", 13, "Trial Registry entry",
				st.source_id, source_name, 12, download_datetime));

			data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
								"Study short name :: object type", true));

			data_object_instances.Add(new ObjectInstance(sd_oid, st.source_id, source_name,
								st.remote_url, true, 35, "Web text"));

			if (registration_date != null)
			{
				data_object_dates.Add(new ObjectDate(sd_oid, 15, "Created", registration_date.year,
						  registration_date.month, registration_date.day, registration_date.date_string));
			}

			if (st.record_date != null)
            {
				SplitDate record_date = DateHelpers.GetDatePartsFromISOString(st.record_date);
				data_object_dates.Add(new ObjectDate(sd_oid, 18, "Updated", record_date.year,
						  record_date.month, record_date.day, record_date.date_string));

			}


			// there may be (rarely) a results link... (exclude those on CTG - should be picked up there)
			if (!string.IsNullOrEmpty(st.results_url_link))
			{
				if (st.results_url_link.Contains("http") && !st.results_url_link.ToLower().Contains("clinicaltrials.gov"))
                {
					object_display_title = name_base + " :: " + "Results summary";
					sd_oid = HashHelpers.CreateMD5(sid + object_display_title);
					SplitDate results_date = null;
					if (st.results_date_posted != null)
					{
						results_date = DateHelpers.GetDatePartsFromISOString(st.results_date_posted);
					}

					int? posted_year = results_date?.year;

					// (in practice may not be in the registry)
					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, posted_year, 
									 23, "Text", 28, "Trial registry results summary",
									 st.source_id, source_name, 12, download_datetime));

					data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
										"Study short name :: object type", true));

					string url_link = Regex.Match(st.results_url_link, @"(http|https)://[\w-]+(\.[\w-]+)+([\w\.,@\?\^=%&:/~\+#-]*[\w@\?\^=%&/~\+#-])?").Value;
					data_object_instances.Add(new ObjectInstance(sd_oid, st.source_id, source_name,
										url_link, true, 35, "Web text"));

					if (results_date != null)
					{
						data_object_dates.Add(new ObjectDate(sd_oid, 15, "Created", results_date.year,
								  results_date.month, results_date.day, results_date.date_string));
					}
				}
			}


			// there may be (rarely) a protocol link...(exclude those simply referring to CTG - should be picked up there)
			if (!string.IsNullOrEmpty(st.results_url_protocol))
			{
				string prot_url = st.results_url_protocol.ToLower();
				if (prot_url.Contains("http") && !prot_url.Contains("clinicaltrials.gov"))
				{
					// presumed to be a download or a web reference
					string resource_type = "";
					int resource_type_id = 0;
					string url_link = "";
					int url_start = prot_url.IndexOf("http");
					if (st.results_url_protocol.Contains(".pdf"))
					{
						resource_type = "PDF";
						resource_type_id = 11;
						int pdf_end = prot_url.IndexOf(".pdf");
						url_link = st.results_url_protocol.Substring(url_start, pdf_end - url_start + 4);

					}
					else if (prot_url.Contains(".doc"))
					{
						resource_type = "Word doc";
						resource_type_id = 16;
						if (prot_url.Contains(".docx"))
						{
							int docx_end = prot_url.IndexOf(".docx");
							url_link = st.results_url_protocol.Substring(url_start, docx_end - url_start + 5);
						}
						else
                        {
							int doc_end = prot_url.IndexOf(".doc");
							url_link = st.results_url_protocol.Substring(url_start, doc_end - url_start + 4);
						}
					}
					else
					{
						// most probably some sort of web reference
						resource_type = "Web text";
						resource_type_id = 35;
						url_link = Regex.Match(st.results_url_protocol, @"(http|https)://[\w-]+(\.[\w-]+)+([\w\.,@\?\^=%&:/~\+#-]*[\w@\?\^=%&/~\+#-])?").Value;
					}

					int object_type_id = 0; string object_type = "";
					if (prot_url.Contains("results summary"))
                    {
						object_type_id = 79;
						object_type = "CSR Summary";
					}
					else
                    {
						object_type_id = 11;
						object_type = "Study Protocol";
					}

					object_display_title = name_base + " :: " + object_type;
					sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

					// almost certainly not in the registry
					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, pub_year, 23, "Text", object_type_id, object_type,
					null, null, 11, download_datetime));

					data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
										"Study short name :: object type", true));

					data_object_instances.Add(new ObjectInstance(sd_oid, st.source_id, source_name,
										url_link, true, resource_type_id, resource_type));
					
				}
			}

			// edit contributors - identify individuals down as organisations

			if (study_contributors.Count > 0)
			{
				foreach (StudyContributor sc in study_contributors)
				{
					if (!sc.is_individual)
					{
						string orgname = sc.organisation_name.ToLower();
						if (IdentifierHelpers.CheckIfIndividual(orgname))
						{
							sc.person_full_name = sc.organisation_name;
							sc.organisation_name = null;
							sc.is_individual = true;
						}
					}
				}
			}

			// add in the study properties
			s.identifiers = study_identifiers;
			s.titles = study_titles;
			s.features = study_features;
			s.topics = study_topics;
			s.contributors = study_contributors;

			s.data_objects = data_objects;
			s.object_titles = data_object_titles;
			s.object_dates = data_object_dates;
			s.object_instances = data_object_instances;

			return s;
		}


		public void StoreData(DataLayer repo, Study s)
		{
			// store study
			StudyInDB st = new StudyInDB(s);
			repo.StoreStudy(st);


			// store study attributes
			if (s.identifiers.Count > 0)
			{
				repo.StoreStudyIdentifiers(StudyCopyHelpers.study_ids_helper,
										  s.identifiers);
			}

			if (s.titles.Count > 0)
			{
				repo.StoreStudyTitles(StudyCopyHelpers.study_titles_helper,
										  s.titles);
			}

			if (s.features.Count > 0)
			{
				repo.StoreStudyFeatures(StudyCopyHelpers.study_features_helper,
										  s.features);
			}

			if (s.topics.Count > 0)
			{
				repo.StoreStudyTopics(StudyCopyHelpers.study_topics_helper,
										  s.topics);
			}

			if (s.contributors.Count > 0)
			{
				repo.StoreStudyContributors(StudyCopyHelpers.study_contributors_helper,
										  s.contributors);
			}


			// store data objects and dataset properties
			if (s.data_objects.Count > 0)
			{
				repo.StoreDataObjects(ObjectCopyHelpers.data_objects_helper,
										 s.data_objects);
			}

			// store data object attributes
			if (s.object_dates.Count > 0)
			{
				repo.StoreObjectDates(ObjectCopyHelpers.object_dates_helper,
										 s.object_dates);
			}

			if (s.object_instances.Count > 0)
			{
				repo.StoreObjectInstances(ObjectCopyHelpers.object_instances_helper,
										 s.object_instances);
			}

			if (s.object_titles.Count > 0)
			{
				repo.StoreObjectTitles(ObjectCopyHelpers.object_titles_helper,
										 s.object_titles);
			}
		}


		private string get_source_name(int? source_id)
		{
			string source_name = "";
			switch (source_id)
			{
				case 100116: { source_name = "Australian New Zealand Clinical Trials Registry"; break; }
				case 100117: { source_name = "Registro Brasileiro de Ensaios Clínicos"; break; }
				case 100118: { source_name = "Chinese Clinical Trial Register"; break; }
				case 100119: { source_name = "Clinical Research Information Service (South Korea)"; break; }
				case 100120: { source_name = "ClinicalTrials.gov"; break; }
				case 100121: { source_name = "Clinical Trials Registry - India"; break; }
				case 100122: { source_name = "Registro Público Cubano de Ensayos Clínicos"; break; }
				case 100123: { source_name = "EU Clinical Trials Register"; break; }
				case 100124: { source_name = "Deutschen Register Klinischer Studien"; break; }
				case 100125: { source_name = "Iranian Registry of Clinical Trials"; break; }
				case 100126: { source_name = "ISRCTN"; break; }
				case 100127: { source_name = "Japan Primary Registries Network"; break; }
				case 100128: { source_name = "Pan African Clinical Trial Registry"; break; }
				case 100129: { source_name = "Registro Peruano de Ensayos Clínicos"; break; }
				case 100130: { source_name = "Sri Lanka Clinical Trials Registry"; break; }
				case 100131: { source_name = "Thai Clinical Trials Register"; break; }
				case 100132: { source_name = "The Netherlands National Trial Register"; break; }
				case 101989: { source_name = "Lebanon Clinical Trials Registry"; break; }
			}
			return source_name;
		}
	}

}

