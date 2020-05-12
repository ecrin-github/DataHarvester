using System;
using System.Collections.Generic;
using System.Linq;

namespace DataHarvester.BioLincc
{
	public class BioLinccProcessor
	{
		HelperFunctions hp;
			
		public BioLinccProcessor()
		{
			hp = new HelperFunctions();
		}

		public Study ProcessData(BioLinccDataLayer repo, BioLinccRecord st)
		{
			Study s = new Study();

			// add in date retrieved in object fetch
			// transfer to study and data object records

			// set up html tag processing for the title fields
			// study display title, title text, object display title

			// add in date processed to log record

			List<StudyIdentifier> study_identifiers = new List<StudyIdentifier>();
			List<StudyTitle> study_titles = new List<StudyTitle>();
			List<StudyReference> study_references = new List<StudyReference>();

			List<DataObject> data_objects = new List<DataObject>();
			List<DataSetProperties> dataset_properties = new List<DataSetProperties>();
			List<DataObjectTitle> data_object_titles = new List<DataObjectTitle>();
			List<DataObjectDate> data_object_dates = new List<DataObjectDate>();
			List<DataObjectInstance> data_object_instances = new List<DataObjectInstance>();

			// need study relationships... possibly not at this stage but after links have been examined...

			string access_details = "Investigators wishing to request materials from studies ... must register (free) on the BioLINCC website. ";
			access_details += "Registered investigators may then request detailed searches and submit an application for data sets ";
			access_details += "and/or biospecimens. (from the BioLINCC website)";

			string de_identification = "All BioLINCC data and biospecimens are de-identified.That is to say that obvious subject identifiers ";
			de_identification += "(e.g., name, addresses, social security numbers, place of birth, city of birth, contact data) ";
			de_identification += "have been redacted from all BioLINCC datasets and biospecimens, and under no circumstances would BioLINCC ";
			de_identification += "provide subject identifiers, or a link to such information, to recipients of coded materials. (from the BioLINCC website)";

			// N.B. defaults of org id, name
			int? sponsor_org_id = 12; string sponsor_org = "No organisation name provided in source data";
			char[] splitter = { '(' };

			// transfer features of main study object
			// In most cases study will have already been registered in CGT

			s.sd_id = st.sd_id;
			s.display_title = st.display_title;
			s.brief_description = st.brief_description;
			s.study_type_id = st.study_type_id;
			s.study_type = st.study_type;
			s.study_status_id = 21;
			s.study_status = "Completed";  // assumption for entry onto web site
			st.study_period = st.study_period.Trim();

			if (st.study_period.Length > 3)
			{
				string first_four = st.study_period.Substring(0, 4);
				if (first_four == first_four.Trim())
				{
					if (Int32.TryParse(first_four, out int start_year))
					{
						s.study_start_year = start_year;
					}
                    else
					{
						// perhaps full month year - e.g. "December 2008..."
						// Get first word
						// Is it a month name? - if so, store the number 
						if (st.study_period.Trim().IndexOf(" ") != -1)
						{
							int spacepos = st.study_period.Trim().IndexOf(" ");
							string month_name = st.study_period.Trim().Substring(0, spacepos);
							if (Enum.TryParse<MonthsFull>(month_name, out MonthsFull month_enum))
							{
								// get value...
								int start_month = (int)month_enum;

								// ...and get next 4 characters - are they a year?
								// if they are it is the start year
								string next_four = st.study_period.Substring(spacepos + 1, 4);
								if (Int32.TryParse(next_four, out start_year))
								{
									s.study_start_month = start_month;
									s.study_start_year = start_year;
								}
							}
						}
					}
				}
			}

			// Add study attribute records.

			// For the study, set up new study identifier = sd_id
			// identifier type = NHBLI ID, id = 42, org = National Heart, Lung, and Blood Institute, id = 100167.

			study_identifiers.Add(new StudyIdentifier(st.sd_id, st.sd_id, 42, "NHLBI ID", 100167, "National Heart, Lung, and Blood Institute (US)"));

			// If there is a NCT ID (there usually is...).
			// if multiple - different sort of relationship; also a group may share the same NCT id
			// to be sorted out once all links aggregated - one to many relationships transfornmed into 'study relationships'

			if (st.registry_ids.Count > 0)
			{
				int n = 0;
				foreach (RegistryId reg_id in st.registry_ids)
				{
					n++;
					study_identifiers.Add(new StudyIdentifier(st.sd_id, reg_id.nct_id, 11, "Trial Registry ID", 100120, "ClinicalTrials.gov"));

					// get sponsor name and organisation using NCT Id using the first id
					if (n == 1)
					{
						var sponsor_details = repo.FetchBioLINCCSponsorFromNCT(reg_id.nct_id);
						sponsor_org_id = sponsor_details.org_id;
						sponsor_org = sponsor_details.org_name;

						// also revise title using nct entry...
						s.display_title = repo.FetchStudyTitle(reg_id.nct_id);
					}
				}
			}

			// For the study, set up two titles, acronym and display title
			// NHLBI title not always exactly the same as the trial registry entry.

			study_titles.Add(new StudyTitle(st.sd_id, st.display_title, 15, "Public Title", true, "From study page on BioLINCC web site"));
			if (!string.IsNullOrEmpty(st.acronym))
			{
				study_titles.Add(new StudyTitle(st.sd_id, st.acronym, 14, "Acronym or Abbreviation", false,""));
			}


			// Create data object records.

			int do_id = 0;

			// For the BioLincc web page, set up new data object, object title, object_instance and object dates

			do_id++;

			// BioLINCC web page 

			int? pub_year = st.publication_year;

			string object_display_title = s.display_title + " :: " + "NHLBI web page";

			data_objects.Add(new DataObject(st.sd_id, do_id, object_display_title, pub_year, 23, "Text", 38, "Study Overview",
				100167, "National Heart, Lung, and Blood Institute (US)", 12));

			data_object_titles.Add(new DataObjectTitle(st.sd_id, do_id, object_display_title, 22,
								"Study short name :: object type", true));

			data_object_instances.Add(new DataObjectInstance(st.sd_id, do_id, 101900, "BioLINCC",
								st.remote_url, true, 35, "Web text"));

			// get date in required format
			// meaning of 'date_prepared' unclear - appears to be later than date last revised
			// leave out for now...
			/*
			if (split_date_prepared != null)
			{
				data_object_dates.Add(new DataObjectDate(st.sd_id, do_id, 15, "Created", split_date_prepared.year,
							split_date_prepared.month, split_date_prepared.day, split_date_prepared.date_string));
			}
			*/

            // Use last_revised_date

			if (st.last_revised_date != null)
			{
				DateTime last_revised = (DateTime)st.last_revised_date;
				data_object_dates.Add(new DataObjectDate(st.sd_id, do_id, 18, "Updated", last_revised.Year,
							last_revised.Month, last_revised.Day, last_revised.ToString("yyyy MMM dd")));
			}

			// If there is a study web site...

			if (st.study_website != "")
			{
				do_id++;
				object_display_title = s.display_title + " :: " + "Study web site";

				data_objects.Add(new DataObject(st.sd_id, do_id, object_display_title, null, 23, "Text", 134, "Website",
									sponsor_org_id, sponsor_org, 12));
				data_object_titles.Add(new DataObjectTitle(st.sd_id, do_id, object_display_title, 22,
									"Study short name :: object type", true));
				data_object_instances.Add(new DataObjectInstance(st.sd_id, do_id, sponsor_org_id, sponsor_org,
									st.study_website, true, 35, "Web text"));
			}


			// create the data object relating to the dataset, instance not available, title possible...
			// may be a description of the data in 'Data Available...'
			// if so add a data object description....with a data object title
			if (st.resources_available.ToLower().Contains("datasets"))
			{
				do_id++;
			    object_display_title = s.display_title + " :: " + "IPD Datasets";
			    data_objects.Add(new DataObject(st.sd_id, do_id, object_display_title, null, 14, "Datasets",
						80, "Individual Participant Data", 100167, "National Heart, Lung, and Blood Institute (US)",
						17, "Case by case download", access_details,
						"https://biolincc.nhlbi.nih.gov/media/guidelines/handbook.pdf?link_time=2019-12-13_11:33:44.807479#page=15",
						DateTime.Now));

			    data_object_titles.Add(new DataObjectTitle(s.sd_id, do_id, object_display_title, 22, "Study short name :: object type", true));

                // Datasets and consent restrictions

			    int consent_type_id = 0;
			    string consent_type = "";
			    string restrictions = "";
				if (string.IsNullOrEmpty(st.dataset_consent_restrictions))
				{
					consent_type_id = 0;
					consent_type = "Not yet known";
					restrictions = "";
				}
				else if (st.dataset_consent_restrictions.ToLower() == "none")
				{
					consent_type_id = 9;
					consent_type = "Comment on consent present";
					restrictions = "Comment only that restrictions on use = None";
				}
				else 
				{
					consent_type_id = 9;
					consent_type = "Comment on consent present";
					restrictions = st.dataset_consent_restrictions;
				}

				// do dataset object separately
				dataset_properties.Add(new DataSetProperties(s.sd_id, do_id, 
					                     3, "Pseudonymised", "",
	                                     3, "Has Indirect Identifiers", de_identification, 
					                     consent_type_id, consent_type, restrictions));
			}


			if (st.primary_docs != null && st.primary_docs.Count > 0)
			{
				foreach (PrimaryDoc pd in st.primary_docs)
				{
					study_references.Add(new StudyReference(s.sd_id, pd.pubmed_id, "", "", "primary"));
				}
			}


			if (st.resources != null && st.resources.Count > 0)
			{
				foreach (Resource r in st.resources)
				{
					// for the resource, set up new data object, object title, object instance
					do_id++;
					object_display_title = s.display_title + " :: " + r.doc_name;
					data_objects.Add(new DataObject(st.sd_id, do_id, object_display_title, pub_year, 23, "Text", r.object_type_id, r.object_type,
										sponsor_org_id, sponsor_org, r.access_type_id));
					data_object_titles.Add(new DataObjectTitle(st.sd_id, do_id, object_display_title, 21, "Study short name :: object name", true));
					data_object_instances.Add(new DataObjectInstance(st.sd_id, do_id, 101900, "BioLINCC", r.url, true, r.doc_type_id, r.doc_type));
				}
			}


			if (st.assoc_docs != null && st.assoc_docs.Count > 0)
			{
				foreach (AssocDoc r in st.assoc_docs)
				{
					study_references.Add(new StudyReference(s.sd_id, r.pubmed_id, r.display_title, r.link_id, "associated"));
				}
			}


			// add in the study properties
			s.identifiers = study_identifiers;
			s.titles = study_titles;
			s.references = study_references;

			s.data_objects = data_objects;
			s.dataset_properties = dataset_properties;
			s.object_titles = data_object_titles;
			s.object_dates = data_object_dates;
			s.object_instances = data_object_instances;

			return s;
		}


		public void StoreData(BioLinccDataLayer repo, Study s)
		{
			// store study
			StudyInDB st = new StudyInDB(s);
			repo.StoreStudy(st);


			// store study attributes
			if (s.identifiers.Count > 0)
			{
				repo.StoreStudyIdentifiers(CopyHelpers.study_ids_helper,
										  s.identifiers);
			}

			if (s.titles.Count > 0)
			{
				repo.StoreStudyTitles(CopyHelpers.study_titles_helper,
										  s.titles);
			}


			if (s.references.Count > 0)
			{
				repo.StoreStudyReferences(CopyHelpers.study_references_helper, 
					                      s.references);
			}


			// store data objects and dataset properties
			if (s.data_objects.Count > 0)
			{
				repo.StoreDataObjects(CopyHelpers.data_objects_helper,
										 s.data_objects);
			}

			if (s.dataset_properties.Count > 0)
			{
				repo.StoreDatasetProperties(CopyHelpers.dataset_properties_helper,
										 s.dataset_properties);
			}

			// store data object attributes
			if (s.object_dates.Count > 0)
			{
				repo.StoreObjectDates(CopyHelpers.object_dates_helper,
										 s.object_dates);
			}

			if (s.object_instances.Count > 0)
			{
				repo.StoreObjectInstances(CopyHelpers.object_instances_helper,
										 s.object_instances);
			}

			if (s.object_titles.Count > 0)
			{
				repo.StoreObjectTitles(CopyHelpers.object_titles_helper,
										 s.object_titles);
			}
		}
	}
}
