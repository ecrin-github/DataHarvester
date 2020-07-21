using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace DataHarvester.who
{
	public class WHOProcessor
	{

		public Study ProcessData(WHORecord st, DateTime? download_datetime, DataLayer common_repo, WHODataLayer biolincc_repo)
		{
			Study s = new Study();

			// get date retrieved in object fetch
			// transfer to study and data object records

			List<StudyIdentifier> study_identifiers = new List<StudyIdentifier>();
			List<StudyTitle> study_titles = new List<StudyTitle>();
			List<StudyFeature> study_features = new List<StudyFeature>();
			List<StudyTopic> study_topics = new List<StudyTopic>();

			List<DataObject> data_objects = new List<DataObject>();
			List<ObjectTitle> data_object_titles = new List<ObjectTitle>();
			List<ObjectDate> data_object_dates = new List<ObjectDate>();
			List<ObjectInstance> data_object_instances = new List<ObjectInstance>();

			// transfer features of main study object
			// In most cases study will have already been registered in CGT

			string sid = st.sd_sid;
			s.sd_sid = sid;
			s.datetime_of_data_fetch = download_datetime;

			// titles
			string public_title = "", scientific_title = "";

			if (!string.IsNullOrEmpty(st.public_title))
            {
				if (st.public_title.Contains("<"))
				{
					public_title = HtmlHelpers.replace_tags(st.public_title);
					public_title = HtmlHelpers.strip_tags(public_title);
				}
				else
				{
					public_title = st.public_title;
				}
			}


			if (!string.IsNullOrEmpty(st.scientific_title))
			{
				if (st.scientific_title.Contains("<"))
				{
					scientific_title = HtmlHelpers.replace_tags(st.scientific_title);
					scientific_title = HtmlHelpers.strip_tags(scientific_title);
				}
				else
				{
					scientific_title = st.scientific_title;
				}
			}

			if (public_title == "")
            {
				if (scientific_title != "")
                {
					study_titles.Add(new StudyTitle(sid, scientific_title, 16, "Trial registry title", true));
					s.display_title = scientific_title;
				}
				else
                {
					s.display_title = "No public or scientific title provided";
				}
			}
			else
            {
				study_titles.Add(new StudyTitle(sid, public_title, 15, "Public Title", true));
				s.display_title = public_title;
				if (scientific_title != "")
				{
					study_titles.Add(new StudyTitle(sid, scientific_title, 16, "Trial registry title", false));
				}
			}

			s.title_lang_code = "en";  // as a default

			// need a mechanism, here tgo try and identify at least majot language variations
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
			}

			if (s.data_sharing_statement.Contains("<"))
			{
				s.data_sharing_statement = HtmlHelpers.replace_tags(s.data_sharing_statement);
				s.dss_contains_html = HtmlHelpers.check_for_tags(s.data_sharing_statement);
			}

			// study start year and month
			// (date of enrolment already in ISO format, if present)
			if (!string.IsNullOrEmpty(st.date_enrollement))
            {
				string year_string = st.date_enrollement.Substring(0, 4);
				string month_string = st.date_enrollement.Substring(5, 2);
				if (Int32.TryParse(year_string, out int year))
                {
					if (year > 1960)
					{
						s.study_start_year = year;
						if (Int32.TryParse(month_string, out int month))
						{
							s.study_start_month = month;
						}
					}
                }
			}


			// study type and status 
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


			// enrolment targets, gender and age groups
			int enrolment = 0;
			if (!string.IsNullOrEmpty(st.results_actual_enrollment)
				&& !st.results_actual_enrollment.Contains("9999"))
			{
				if (Regex.Match(st.results_actual_enrollment, @"\d+").Success)
                {
					enrolment = Int32.Parse(Regex.Match(st.results_actual_enrollment, @"\d+").Value);
				}
			}

			// use the target if that is all that is available
			if (enrolment == 0 && !string.IsNullOrEmpty(st.target_size)
				&& !st.target_size.Contains("9999"))
			{
				if (Regex.Match(st.target_size, @"\d+").Success)
				{
					enrolment = Int32.Parse(Regex.Match(st.target_size, @"\d+").Value);
				}
			}


			if (Int32.TryParse(st.agemin, out int min))
            {
				s.min_age = min;
				s.min_age_units = st.agemin_units;
				s.min_age_units_id = TypeHelpers.GetTimeUnitsId(s.min_age_units);
			}

			if (Int32.TryParse(st.agemax, out int max))
			{
				s.max_age = max;
				s.max_age_units = st.agemax_units;
				s.max_age_units_id = TypeHelpers.GetTimeUnitsId(s.max_age_units);
			}

			s.study_gender_elig = st.gender;
			s.study_gender_elig_id = TypeHelpers.GetGenderEligId(st.gender);

			// Add study attribute records.



			// study features and study identifiers, conditions




			// study contributors
			// sponsor and scientific lead
			// N.B. defaults of org id, name
			int? sponsor_org_id = 12; string sponsor_org = "No organisation name provided in source data";



			// Create data object records.
			// registry entry




			// there may be a results link...




			// For the BioLincc web page, set up new data object, object title, object_instance and object dates
			//int? pub_year = st.publication_year;
			string name_base = s.display_title;
			string object_display_title = name_base + " :: " + "NHLBI web page";

			// create hash Id for the data object
			string sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

			//data_objects.Add(new DataObject(sd_oid, sid, object_display_title, pub_year, 23, "Text", 38, "Study Overview",
			//	100167, "National Heart, Lung, and Blood Institute (US)", 12, download_datetime));

			data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
								"Study short name :: object type", true));

			data_object_instances.Add(new ObjectInstance(sd_oid, 101900, "BioLINCC",
								st.remote_url, true, 35, "Web text"));

			


            // Use last_revised_date

			if (st.record_date != null)
			{
				//DateTime last_revised = (DateTime)st.record_date;
				//data_object_dates.Add(new ObjectDate(sd_oid, 18, "Updated", last_revised.Year,
				//			last_revised.Month, last_revised.Day, last_revised.ToString("yyyy MMM dd")));
			}

			

			
			List<StudyReference> study_references2 = new List<StudyReference>();
			foreach (StudyReference a in study_references)
			{
				if (a.comments != "to go")
				{
					study_references2.Add(a);
				}
			}

			// add in the study properties
			s.identifiers = study_identifiers;
			s.titles = study_titles;
			s.features = study_features;
			s.topics = study_topics;

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


			if (s.references.Count > 0)
			{
				repo.StoreStudyReferences(StudyCopyHelpers.study_references_helper, 
					                      s.references);
			}


			// store data objects and dataset properties
			if (s.data_objects.Count > 0)
			{
				repo.StoreDataObjects(ObjectCopyHelpers.data_objects_helper,
										 s.data_objects);
			}

			if (s.dataset_properties.Count > 0)
			{
				repo.StoreDatasetProperties(ObjectCopyHelpers.dataset_properties_helper,
										 s.dataset_properties);
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
	}
}
