using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Threading.Tasks;

namespace DataHarvester.euctr
{
	public class EUCTRProcessor
	{
		HtmlHelpers hhp;
		HashHelpers hf;

		public EUCTRProcessor()
		{
			hhp = new HtmlHelpers();
			hf = new HashHelpers();
		}

		public async Task<Study> ProcessDataAsync(EUCTR_Record fs, DateTime? download_datetime, DataLayer common_repo)
		{
			Study s = new Study();
			List<StudyIdentifier> identifiers = new List<StudyIdentifier>();
			List<StudyTitle> titles = new List<StudyTitle>();
			List<StudyContributor> contributors = new List<StudyContributor>();
			List<StudyReference> references = new List<StudyReference>();
			List<StudyLink> studylinks = new List<StudyLink>();
			List<StudyTopic> topics = new List<StudyTopic>();
			List<StudyFeature> features = new List<StudyFeature>();

			List<DataObject> data_objects = new List<DataObject>();
			List<DataObjectTitle> object_titles = new List<DataObjectTitle>();
			List<DataObjectDate> object_dates = new List<DataObjectDate>();
			List<DataObjectInstance> object_instances = new List<DataObjectInstance>();

			// STUDY AND ATTRIBUTES

			string sid = fs.eudract_id;
			s.sd_sid = sid;
			s.datetime_of_data_fetch = download_datetime;

			// By defintion with the EU CTR
			s.study_type = "Interventional";
			s.study_type_id = 11;

			s.study_status = fs.trial_status;
			switch (fs.trial_status)
			{
				case "Ongoing":
					{
						s.study_status = "Ongoing";
						s.study_status_id = 25; 
						break;
					}
				case "Completed":
					{
						s.study_status = "Completed";
						s.study_status_id = 21;
						break;
					}
				case "Prematurely Ended":
					{
						s.study_status = "Terminated";
						s.study_status_id = 22;
						break;
					}
				case "Temporarily Halted":
					{
						s.study_status = "Suspended";
						s.study_status_id = 18;
						break;
					}
				case "Not Authorised":
					{
						s.study_status = "Withdrawn";
						s.study_status_id = 11;
						break;
					}
				default:
					{
						s.study_status = fs.trial_status;
						s.study_status_id = 0;
						break;
					}
			}


				
			// study start year and month
			// public string start_date { get; set; }  in yyyy-MM-dddd format
			if (DateTime.TryParseExact(fs.start_date, "yyyy-MM-dd", new CultureInfo("en-UK"), DateTimeStyles.AssumeLocal, out DateTime start))
			{
				s.study_start_year = start.Year;
				s.study_start_month = start.Month;
	        }

			// contributor - sponsor
			string study_sponsor = fs.sponsor_name;
			contributors.Add(new StudyContributor(sid, 54, "Trial Sponsor", null, study_sponsor, null, null));
			
			// may get funders or other supporting orgs
			if (fs.sponsors.Count > 0)
			{
				//fs.funders.Sort();
				foreach (DetailLine i in fs.sponsors)
				{
					switch (i.item_name)
					{
						case "Name of organisation providing support":
							{
								// check a funder is not simply the sponsor...
								if (i.item_values[0].value != study_sponsor)
								{
									string funder = i.item_values[0].value;
									contributors.Add(new StudyContributor(sid, 58, "Study Funder", null, funder, null, null));
								}
								break;
							}

						default:
							{
								// do nothing
								break;
							}
					}
				}
			}


			// study identifiers
			// do the eu ctr id first...
			identifiers.Add(new StudyIdentifier(sid, fs.eudract_id, 11, "Trial Registry ID", 100123, "EU Clinical Trials Register", null, null));

			// do the sponsor's id
			identifiers.Add(new StudyIdentifier(sid, fs.sponsor_id, 14, "Sponsor ID", null, study_sponsor, null, null));


			// identifier section actually seems to have titles
			if (fs.identifiers.Count > 0)
			{
				foreach (DetailLine i in fs.identifiers)
				{
					switch (i.item_code)
					{
						case "A.3":
							{
								// may be multiple
								foreach(item_value n in i.item_values)
								{
									titles.Add(new StudyTitle(sid, n.value, 16, "Trial Registry title", false));
								}
								break;
							}
						case "A.3.1":
							{
								// may be multiple
								int k = 0;
								foreach (item_value n in i.item_values)
								{
									k++;
									if (k==1)
									{
										titles.Add(new StudyTitle(sid, n.value, 15, "Public title", true));
										s.display_title = n.value;
									}
									else
									{
										titles.Add(new StudyTitle(sid, n.value, 15, "Public title", false));
									}
								}
								break;
							}
						case "A.3.2":
							{
								string topic_name = i.item_values[0].value;
								string name = topic_name.ToLower();
								if (!name.StartsWith("not ") && name != "n/a" && name != "na" && name != "-" && name != "--")
								{
									titles.Add(new StudyTitle(sid, topic_name, 14, "Acronym or Abbreviation", false));
								}
								break;
							}
						case "A.1":
							{
								// do nothing
								break;
							}
						case "A.2":
							{
								// do nothing
								break;
							}
						case "A.4.1":
							{
								// do nothing - already have sponsor id
								break;
							}
						case "A.5.1":
							{
								// identifier: ISRCTN (International Standard Randomised Controlled Trial) Number
								if (i.item_values[0].value.ToLower().StartsWith("isrctn"))
								{
									identifiers.Add(new StudyIdentifier(sid, i.item_values[0].value, 11, "Trial Registry ID", 
										100126, "ISRCTN", null, null));
								}
								break;
							}
						case "A.5.2":
							{
								// identifier: NCT Number
								if (i.item_values[0].value.ToLower().StartsWith("nct"))
								{
									identifiers.Add(new StudyIdentifier(sid, i.item_values[0].value, 11, "Trial Registry ID",
										100120, "ClinicalTrials.gov", null, null));
								}
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "identifier: " + i.item_code + " " + i.item_name, i.item_values[0].value));
								break;
							}
					}
				}
			}

			// ensure a default and display title
			bool default_title_exists = false;
			for (int k = 0; k < titles.Count; k++)
			{
				if (titles[k].is_default)
				{
					default_title_exists = true;
					break;
				}
			}

			if (!default_title_exists)
			{
				// use a scientific title - should always be one
				for (int k = 0; k < titles.Count; k++)
				{
					if (titles[k].title_type_id == 16)
					{
						titles[k].is_default = true;
						s.display_title = titles[k].title_text;
						break;
					}
				}
			}

			// add in an explanatory message... if no title
			

			// study design info
			if (fs.features.Count > 0)
			{
				foreach (DetailLine i in fs.features)
				{
					switch (i.item_code)
					{
						case "E.1.1":
							{
								// conditions under study
								foreach (item_value n in i.item_values)
								{
									topics.Add(new StudyTopic(sid, 13, "condition", n.value, null, "condition under study"));
								}
								break;
							}
						case "E.2.1":
							{
								// primary objectives
								string objectives;
								if (i.item_values[0].value.StartsWith("Primary"))
								{
									objectives = i.item_values[0].value;
								}
								else
								{
									objectives = "Primary objectives: " + i.item_values[0].value;
								}
								s.brief_description = objectives;
								break;
							}
						//case "E.5.1":
						//	{
						//		// primary end points

						//		string end_points;
						//		if (i.item_values[0].value.StartsWith("Primary"))
						//		{
						//			end_points = i.item_values[0].value;
						//		}
						//		else
						//		{
						//			end_points = "Primary endpoints: " + i.item_values[0].value;
						//		}
						//		if (string.IsNullOrEmpty(s.brief_description))
						//		{
						//			s.brief_description = end_points;
						//		}
						//		else
						//		{
						//			s.brief_description += " ";
						//			s.brief_description += end_points;
						//		}
						//		break;
						//	}
						case "E.7.1":
							{
								// Phase 1
								features.Add(new StudyFeature(sid, 20, "phase", 110, "Phase 1"));
								break;
							}
						case "E.7.2":
							{
								// Phase 2
								features.Add(new StudyFeature(sid, 20, "phase", 120, "Phase 2"));
								break;
							}
						case "E.7.3":
							{
								// Phase 3
								features.Add(new StudyFeature(sid, 20, "phase", 130, "Phase 3"));
								break;
							}
						case "E.7.4":
							{
								// Phase 4
								features.Add(new StudyFeature(sid, 20, "phase", 135, "Phase 4"));
								break;
							}
						case "E.8.1":
							{
								// Controlled - do nothing
								break;
							}
						case "E.8.1.1":
							{
								// Randomised
								features.Add(new StudyFeature(sid, 22, "allocation type", 205, "Randomised"));
								break;
							}
						case "E.8.1.2":
							{
								// open
								features.Add(new StudyFeature(sid, 24, "masking", 500, "None (Open Label)"));
								break;
							}
						case "E.8.1.3":
							{
								// Single blindd
								features.Add(new StudyFeature(sid, 24, "masking", 505, "Single"));
								break;
							}
						case "E.8.1.4":
							{
								// Double blind
								features.Add(new StudyFeature(sid, 24, "masking", 510, "Double"));
								break;
							}
						case "E.8.1.5":
							{
								// Parallel group
								features.Add(new StudyFeature(sid, 23, "intervention model", 305, "Parallel assignment"));
								break;
							}
						case "E.8.1.6":
							{
								// Crossover
								features.Add(new StudyFeature(sid, 23, "intervention model", 310, "Crossover assignment"));
								break;
							}
						default:
							{
								// do nothing
								break;
							}
					}
				}
			}

			// eligibility
			if (fs.population.Count > 0)
			{
				bool includes_under18 = false;
				bool includes_in_utero = false, includes_preterm = false;
				bool includes_newborns = false, includes_infants= false;
				bool includes_children = false, includes_ados = false;
				bool includes_adults = false, includes_elderly = false;
				bool includes_women = false, includes_men = false;

				foreach (DetailLine i in fs.population)
				{
					switch (i.item_code)
					{
						case "F.1.1":
							{
								// under 18
								includes_under18 = true; break;
							}
					    case "F.1.1.1":
							{
								includes_in_utero = true;  break;
							}
						case "F.1.1.2":
							{
								includes_preterm = true; break;
							}
						case "F.1.1.3":
							{
								includes_newborns = true; break;
							}
						case "F.1.1.4":
							{
								includes_infants = true; break;
							}
						case "F.1.1.5":
							{
								includes_children = true; break;
							}
						case "F.1.1.6":
							{
								includes_ados = true; break;
							}

						case "F.1.2":
							{
								// Adults 18 - 64
								includes_adults = true;	break;
							}
						case "F.1.3":
							{
								// Elderly, >65
								includes_elderly = true; break;
							}
						case "F.2.1":
							{
								includes_women = true; break;
							}
						case "F.2.2":
							{
								includes_men = true; break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "population: " + i.item_name, i.item_values[0].value));
								break;
							}
					}
				}

				if (includes_men && includes_women)
				{
					s.study_gender_elig = "All"; s.study_gender_elig_id = 900;
				}
				else if (includes_women)
				{
					s.study_gender_elig = "Female"; s.study_gender_elig_id = 905;
				}
				else if (includes_men)
				{
					s.study_gender_elig = "Male"; s.study_gender_elig_id = 910;
				}

				if (!includes_under18)
				{
					if (includes_adults && includes_elderly)
					{
						s.min_age = 18;	s.min_age_units = "Years"; s.min_age_units_id = 17;
					}
					else if (includes_adults)
					{
						s.min_age = 18; s.min_age_units = "Years"; s.min_age_units_id = 17;
						s.max_age = 64; s.max_age_units = "Years"; s.max_age_units_id = 17;
					}
					else if (includes_elderly)
					{
						s.min_age = 65; s.min_age_units = "Years"; s.min_age_units_id = 17;
					}
				}
				else
				{
					if (includes_in_utero || includes_preterm || includes_newborns)
					{
						s.min_age = 0; s.min_age_units = "Days"; s.min_age_units_id = 14;
					}
					else if (includes_infants)
					{
						s.min_age = 28; s.min_age_units = "Days"; s.min_age_units_id = 14;
					}
					else if (includes_children)
					{
						s.min_age = 2; s.min_age_units = "Years"; s.min_age_units_id = 17;
					}
					else if (includes_ados)
					{
						s.min_age = 12; s.min_age_units = "Years"; s.min_age_units_id = 17;
					}


					if (includes_adults)
					{
						s.max_age = 64; s.max_age_units = "Years"; s.max_age_units_id = 17;
					}
					else if (includes_ados)
					{
						s.max_age = 17; s.max_age_units = "Years"; s.max_age_units_id = 17;
					}
					else if (includes_children)
					{
						s.max_age = 11; s.max_age_units = "Years"; s.max_age_units_id = 17;
					}
					else if (includes_infants)
					{
						s.max_age = 23; s.max_age_units = "Months"; s.max_age_units_id = 16;
					}
					else if (includes_newborns)
					{
						s.max_age = 27; s.max_age_units = "Days"; s.max_age_units_id = 14;
					}
					else if (includes_in_utero || includes_preterm)
					{
						s.max_age = 0; s.max_age_units = "Days"; s.max_age_units_id = 14;
					}
				}

			}

			// for topics
			//public string medical_condition { get; set; }

			// for topics
			// public List<ImpLine> imps { get; set; }
			if (fs.imps.Count > 0)
			{
				string name, topic_name;					
				foreach (ImpLine i in fs.imps)
				{

					switch (i.item_code)
					{
						case "D.2.1.1.1":
							{
								// Trade name
								topic_name = i.item_values[0].value;
								name = topic_name.ToLower();
								if (name != "not available" && name != "n/a" && name != "na" && name != "not yet extablished")
								{
									// but is it already there?
									bool new_topic = true;
									foreach(StudyTopic t in topics)
									{
										if (t.topic_value == topic_name)
										{
											new_topic = false; break;
										}
									}
									if (new_topic)
									{
										topics.Add(new StudyTopic(sid, 12, "chemical / agent", topic_name, null, "trade name"));
									}
								}
								break;
							}
						case "D.3.1":
							{
								// Product name
								topic_name = i.item_values[0].value;
								name = topic_name.ToLower();
								if (name != "not available" && name != "n/a" && name != "na" && name != "not yet extablished")
								{
									// but is it already there?
									bool new_topic = true;
									foreach (StudyTopic t in topics)
									{
										if (t.topic_value == topic_name)
										{
											new_topic = false; break;
										}
									}
									if (new_topic)
									{
										topics.Add(new StudyTopic(sid, 12, "chemical / agent", i.item_values[0].value, null, "product name"));
									}
								}
								break;
							}
						case "D.3.8":
							{
								// INN
								topic_name = i.item_values[0].value;
								name = topic_name.ToLower();
								if (name != "not available" && name != "n/a" && name != "na" && name != "not yet extablished")
								{
									// but is it already there?
									bool new_topic = true;
									foreach (StudyTopic t in topics)
									{
										if (t.topic_value == topic_name)
										{
											new_topic = false; break;
										}
									}
									if (new_topic)
									{
										topics.Add(new StudyTopic(sid, 12, "chemical / agent", i.item_values[0].value, null, "INN or proposed INN"));
									}
								}
								break;
							}
						case "D.3.9.1":
							{
								// CAS number, do nothing
								break;
							}
						case "D.3.9.3":
							{
								// other ddescriptive name, do nothing
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "imps: " + i.item_name, i.item_values[0].value));
								break;
							}
					}
				}
			}


			// public List<MeddraTerm> meddra_terms { get; set; }
			if (fs.meddra_terms.Count > 0)
			{
				foreach (MeddraTerm i in fs.meddra_terms)
				{   
					string med_version, code, level, term;
					if (!string.IsNullOrEmpty(i.term))
					{
						term = i.term;
						code = (string.IsNullOrEmpty(i.code)) ? "" : i.code;
						med_version = (string.IsNullOrEmpty(i.version)) ? "" : i.version;
						level = (string.IsNullOrEmpty(i.level)) ? "" : i.level;

						topics.Add(new StudyTopic(sid, 13, "condition", term, 
							     ("MedDRA " + med_version + " " + level).Trim(), "meddra"));
					}
				}
			}

			// not used at present
			//public string competent_authority { get; set; }

			// DATA OBJECTS and their attributes
			// initial data object is the EUCTR registry entry

			string object_display_title = s.display_title + " :: EU CTR registry entry";

			// create hash Id for the data object
			string sd_oid = hf.CreateMD5(sid + object_display_title);

			data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
				  23, "Text", 13, "Trial Registry entry", 100123, "EU Clinical Trials Register", 
				  12, download_datetime));

			// data object title is the single display title...
			object_titles.Add(new DataObjectTitle(sd_oid, object_display_title,
											 22, "Study short name :: object type", true));
			

			// instance url can be derived from the EUCTR number
			object_instances.Add(new DataObjectInstance(sd_oid, 100123, "EU Clinical Trials Register",
						fs.details_url, true, 35, "Web text"));

			// if there is a results url, add that in as well
			if (!string.IsNullOrEmpty(fs.results_url))
			{
				object_display_title = s.display_title + " :: EU CTR results entry";
				sd_oid = hf.CreateMD5(sid + object_display_title);

				data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
					  23, "Text", 28, "Trial registry results summary", 100123, 
					  "EU Clinical Trials Register", 12, download_datetime));

				// data object title is the single display title...
				object_titles.Add(new DataObjectTitle(sd_oid, object_display_title,
														 22, "Study short name :: object type", true));

				// instance url can be derived from the ISRCTN number
				object_instances.Add(new DataObjectInstance(sd_oid,  100123, "EU Clinical Trials Register",
							fs.results_url, true, 36, "Web text with download"));
			}

			s.identifiers = identifiers;
			s.titles = titles;
			s.contributors = contributors;
			s.references = references;
			s.studylinks = studylinks;
			s.topics = topics;
			s.features = features;

			s.data_objects = data_objects;
			s.object_titles = object_titles;
			s.object_dates = object_dates;
			s.object_instances = object_instances;

			return s;

		}


		
		public void StoreData(DataLayer repo, Study s)
		{
			// construct database study instance
			StudyInDB dbs = new StudyInDB(s);

			dbs.study_enrolment = s.study_enrolment;
			dbs.study_gender_elig_id = s.study_gender_elig_id;
			dbs.study_gender_elig = s.study_gender_elig;

			dbs.min_age = s.min_age;
			dbs.min_age_units_id = s.min_age_units_id;
			dbs.min_age_units = s.min_age_units;
			dbs.max_age = s.max_age;
			dbs.max_age_units_id = s.max_age_units_id;
			dbs.max_age_units = s.max_age_units;

			repo.StoreStudy(dbs);

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

			if (s.contributors.Count > 0)
			{
				repo.StoreStudyContributors(CopyHelpers.study_contributors_helper,
										  s.contributors);
			}

			if (s.studylinks.Count > 0)
			{
				repo.StoreStudyLinks(CopyHelpers.study_links_helper,
										  s.studylinks);
			}

			if (s.topics.Count > 0)
			{
				repo.StoreStudyTopics(CopyHelpers.study_topics_helper,
										  s.topics);
			}

			if (s.features.Count > 0)
			{
				repo.StoreStudyFeatures(CopyHelpers.study_features_helper,
										  s.features);
			}

			//if (s.ipd_info.Count > 0) r.StoreIPDInfo(ipd_copyhelper, s.ipd_info);

			//if (s.relationships.Count > 0) r.StoreRelationships(relationship_copyhelper, s.relationships);

			if (s.data_objects.Count > 0)
			{
				repo.StoreDataObjects(CopyHelpers.data_objects_helper,
										  s.data_objects);
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

			if (s.object_dates.Count > 0)
			{
				repo.StoreObjectDates(CopyHelpers.object_dates_helper,
										  s.object_dates);
			}

		}

	}

	
	public class URLChecker
	{
		HttpClient Client = new HttpClient();
		DateTime today = DateTime.Today;

		public async Task CheckURLsAsync(List<DataObjectInstance> web_resources)
		{
			foreach (DataObjectInstance i in web_resources)
			{
				if (i.resource_type_id == 11)  // just do the study docs for now (pdfs)
				{
					string url_to_check = i.url;
					if (url_to_check != null && url_to_check != "")
					{
                        HttpRequestMessage http_request = new HttpRequestMessage(HttpMethod.Head, url_to_check);
                        var result = await Client.SendAsync(http_request);
                        if ((int)result.StatusCode == 200)
                        {
                            i.url_last_checked = today;
                        }
                    }
				}
			}
		}


		public async Task<bool> CheckURLAsync(string url_to_check)
		{
			if (!string.IsNullOrEmpty(url_to_check))
			{
				try
				{
					HttpRequestMessage http_request = new HttpRequestMessage(HttpMethod.Head, url_to_check);
					var result = await Client.SendAsync(http_request);
					return ((int)result.StatusCode == 200);
				}
				catch (Exception e)
				{
					string message = e.Message;
					return false;
				}
			}
			else
			{
				return false;
			}
		}
	}

}
