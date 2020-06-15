using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Threading.Tasks;

namespace DataHarvester.euctr
{
	public class EUCTRProcessor
	{
		HtmlHelperFunctions hhp;
		HelperFunctions hf;

		public EUCTRProcessor()
		{
			hhp = new HtmlHelperFunctions();
			hf = new HelperFunctions();
		}

		public async Task<Study> ProcessDataAsync(EUCTR_Record fs, DateTime? download_datetime, DataLayer common_repo)
		{
			Study s = new Study();
			List<StudyIdentifier> identifiers = new List<StudyIdentifier>();
			List<StudyTitle> titles = new List<StudyTitle>();
			List<StudyContributor> contributors = new List<StudyContributor>();
			//List<Reference> references = new List<Reference>();
			List<StudyLink> studylinks = new List<StudyLink>();
			List<StudyTopic> topics = new List<StudyTopic>();
			List<StudyFeature> features = new List<StudyFeature>();

			List<DataObject> data_objects = new List<DataObject>();
			List<DataObjectTitle> object_titles = new List<DataObjectTitle>();
			//List<DataObjectDate> object_dates = new List<DataObjectDate>();
			List<DataObjectInstance> object_instances = new List<DataObjectInstance>();

			// STUDY AND ATTRIBUTES

			string sid = fs.eudract_id;
			s.sd_sid = sid;

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
			//s.references = references;
			s.studylinks = studylinks;
			s.topics = topics;
			s.features = features;

			s.data_objects = data_objects;
			s.object_titles = object_titles;
			//s.object_dates = object_dates;
			s.object_instances = object_instances;

			return s;

		}



		// A helper function called from the loop that goes through the secondary Id data
		// It tries to make the data as complete as possible, depending on the typem of 
		// secondary id that is being processed
		void GetIdentifierProps(object[] items, out string id_type, out string id_org,
								out int? id_type_id, out int? id_org_id)
		{
			//default values
			id_type = "SecondaryIdType";
			id_org = TidyPunctuation("SecondaryIdDomain");

			id_type_id = null;
			id_org_id = null;

			if (id_org == null)
			{
				id_org = "No organisation name provided in source data";
				id_org_id = 12;
			}

			if (id_type == null)
			{
				id_type_id = 1;
				id_type = "No type given in source data";
			}

			if (id_type == "Other Identifier")
			{
				id_type_id = 90;
				id_type = "Other";
			}

			if (id_type == "U.S. NIH Grant/Contract")
			{
				id_org_id = 100134;
				id_org = "National Institutes of Health";
				id_type_id = 13;
				id_type = "Funder’s ID";
			}

			if (id_type == "Other Grant/Funding Number")
			{
				id_type_id = 13;
				id_type = "Funder’s ID";
			}

			if (id_type == "EudraCT Number")
			{
				id_org_id = 100123;
				id_org = "EU Clinical Trials Register";
				id_type_id = 11;
				id_type = "Trial Registry ID";
			}

			if (id_type == "Registry Identifier")
			{
				id_type_id = 11;
				id_type = "Trial Registry ID";
				id_org = id_org.ToLower();

				if (id_org.Contains("ctrp") || id_org.Contains("pdq") || id_org.Contains("nci"))
				{
					// NCI CTRP programme
					id_org_id = 100162;
					id_org = "National Cancer Institute";
					id_type_id = 39;
					id_type = "NIH CTRP ID";
				}

				else if (id_org.Contains("daids"))
				{
					// NCI CTRP programme
					id_org_id = 100168;
					id_org = "National Institute of Allergy and Infectious Diseases";
					id_type_id = 40;
					id_type = "DAIDS ID";
				}

				else if (id_org.Contains("who") || id_org.Contains("utn") || id_org.Contains("universal"))
				{
					// NCI CTRP programme
					id_org_id = 100115;
					id_org = "International Clinical Trials Registry Platform";
				}

				else if (id_org.Contains("japic") || id_org.Contains("cti"))
				{
					// japanese registry
					id_org_id = 100157;
					id_org = "Japan Pharmaceutical Information Center";
				}

				else if (id_org.Contains("umin"))
				{
					// japanese registry
					id_org_id = 100156;
					id_org = "University Hospital Medical Information Network CTR";
				}

				else if (id_org.Contains("isrctn"))
				{
					// japanese registry
					id_org_id = 100126;
					id_org = "ISRCTN";
				}

				else if (id_org.Contains("india") || id_org.Contains("ctri"))
				{
					// japanese registry
					id_org_id = 100121;
					id_org = "Clinical Trials Registry - India";
				}

				else if (id_org.Contains("eudract"))
				{
					// japanese registry
					id_org_id = 100123;
					id_org = "EU Clinical Trials Register";
				}

				else if (id_org.Contains("drks") || id_org.Contains("german") || id_org.Contains("deutsch"))
				{
					// japanese registry
					id_org_id = 100124;
					id_org = "Deutschen Register Klinischer Studien";
				}

				else if (id_org.Contains("nederlands") || id_org.Contains("dutch"))
				{
					// japanese registry
					id_org_id = 100132;
					id_org = "The Netherlands National Trial Register";
				}

				else if (id_org.Contains("ansm") || id_org.Contains("agence") || id_org.Contains("rcb"))
				{
					// french asnsm number=
					id_org_id = 101408;
					id_org = "Agence Nationale de Sécurité du Médicament";
					id_type_id = 41;
					id_type = "Regulatory Body ID";
				}


				else if (id_org.Contains("iras") || id_org.Contains("hra"))
				{
					// uk IRAS number
					id_org_id = 101409;
					id_org = "Health Research Authority";
					id_type_id = 41;
					id_type = "Regulatory Body ID";
				}

				else if (id_org.Contains("anzctr") || id_org.Contains("australian"))
				{
					// australian registry
					id_org_id = 100116;
					id_org = "Australian New Zealand Clinical Trials Registry";
				}

				else if (id_org.Contains("chinese"))
				{
					// chinese registry
					id_org_id = 100118;
					id_org = "Chinese Clinical Trial Register";
				}

				else if (id_org.Contains("thai"))
				{
					// thai registry
					id_org_id = 100131;
					id_org = "Thai Clinical Trials Register";
				}

				if (id_org == "JHMIRB" || id_org == "JHM IRB")
				{
					// ethics approval number
					id_org_id = 100190;
					id_org = "Johns Hopkins University";
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}

				if (id_org.ToLower().Contains("ethics") || id_org == "Independent Review Board" || id_org.Contains("IRB"))
				{
					// ethics approval number
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}
			}

			if (id_type_id == 1 || id_type_id == 90)
			{
				string id_value = "SecondaryId";

				if (id_org == "UTN")
				{
					// NCI CTRP programme
					id_org_id = 100115;
					id_org = "International Clinical Trials Registry Platform";
					id_type_id = 11;
					id_type = "Trial Registry ID";
				}

				if (id_org.ToLower().Contains("ansm") || id_org.ToLower().Contains("rcb"))
				{
					// NCI CTRP programme
					id_org_id = 101408;
					id_org = "Agence Nationale de Sécurité du Médicament";
					id_type_id = 41;
					id_type = "Regulatory Body ID";
				}

				if (id_org == "JHMIRB" || id_org == "JHM IRB")
				{
					// ethics approval number
					id_org_id = 100190;
					id_org = "Johns Hopkins University"; 
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}

				if (id_org.ToLower().Contains("ethics") || id_org == "Independent Review Board" || id_org.Contains("IRB"))
				{
					// ethics approval number
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}

				if (id_value.Length > 4 && id_value.Substring(0, 4) == "NCI-")
				{
					// ethics approval number
					id_org_id = 100162;
					id_org = "National Cancer Institute";
				}

				// need a mechanism here to find the org id and system name for the organisation as given
				// probably better to do it in a bulk operation on transfer of the data to the ad tables
			}
		}


		SplitDate GetDateParts(string dateString)
		{
			// input date string is in the form of "<month name> day, year"
			// or in some cases in the form "<month name> year"
			// split the string on the comma
			string year_string, month_name, day_string;
			int? year_num, month_num, day_num;

			int comma_pos = dateString.IndexOf(',');
			if (comma_pos > 0)
			{
				year_string = dateString.Substring(comma_pos + 1).Trim();
				string first_part = dateString.Substring(0, comma_pos).Trim();

				// first part should split on the space
				int space_pos = first_part.IndexOf(' ');
				day_string = first_part.Substring(space_pos + 1).Trim();
				month_name = first_part.Substring(0, space_pos).Trim();
			}
			else
			{
				int space_pos = dateString.IndexOf(' ');
				year_string = dateString.Substring(space_pos + 1).Trim();
				month_name = dateString.Substring(0, space_pos).Trim();
				day_string = "";
			}

			// convert strings into integers
			if (int.TryParse(year_string, out int y)) year_num = y; else year_num = null;
			month_num = GetMonthAsInt(month_name);
			if (int.TryParse(day_string, out int d)) day_num = d; else day_num = null;
			string month_as3 = ((Months3)month_num).ToString();

			// get date as string
			string date_as_string;
			if (year_num != null && month_num != null && day_num != null)
			{
				date_as_string = year_num.ToString() + " " + month_as3 + " " + day_num.ToString();
			}
			else if (year_num != null && month_num != null && day_num == null)
			{
				date_as_string = year_num.ToString() + ' ' + month_as3;
			}
			else if (year_num != null && month_num == null && day_num == null)
			{
				date_as_string = year_num.ToString();
			}
			else
			{
				date_as_string = null;
			}

			return new SplitDate(year_num, month_num, day_num, date_as_string);
		}

		string StandardiseDateFormat(string inputDate)
		{
			SplitDate SD = GetDateParts(inputDate);
			return SD.date_string;
		}


		int? GetMonthAsInt(string month_name)
		{
			return (int)(Enum.Parse<MonthsFull>(month_name));
		}


		int? GetStatusId(string study_status)
		{
			int? type_id = null;
			switch (study_status.ToLower())
			{
				case "completed": type_id = 21; break;
				case "recruiting": type_id = 14; break;
				case "active, not recruiting": type_id = 15; break;
				case "not yet recruiting": type_id = 16; break;
				case "unknown status": type_id = 0; break;
				case "withdrawn": type_id = 11; break;
				case "available": type_id = 12; break;
				case "withheld": type_id = 13; break;
				case "no longer available": type_id = 17; break;
				case "suspended": type_id = 18; break;
				case "enrolling by invitation": type_id = 19; break;
				case "approved for marketing": type_id = 20; break;
				case "terminated": type_id = 22; break;
			}
			return type_id;
		}

		int? GetTypeId(string study_type)
		{
			int? type_id = null;
			switch (study_type.ToLower())
			{
				case "interventional": type_id = 11; break;
				case "observational": type_id = 12; break;
				case "observational patient registry": type_id = 13; break;
				case "expanded access": type_id = 14; break;
				case "funded programme": type_id = 15; break;
				case "not yet known": type_id = 0; break;
			}
			return type_id;
		}

		int? GetGenderEligId(string gender_elig)
		{
			int? type_id = null;
			switch (gender_elig.ToLower())
			{
				case "all": type_id = 900; break;
				case "female": type_id = 905; break;
				case "male": type_id = 910; break;
				case "not provided": type_id = 915; break;
			}
			return type_id;
		}

		int? GetTimeUnitsId(string time_units)
		{
			int? type_id = null;
			switch (time_units.ToLower())
			{
				case "seconds": type_id = 11; break;
				case "minutes": type_id = 12; break;
				case "hours": type_id = 13; break;
				case "days": type_id = 14; break;
				case "weeks": type_id = 15; break;
				case "months": type_id = 16; break;
				case "years": type_id = 17; break;
				case "not provided": type_id = 0; break;
			}
			return type_id;
		}

		int? GetPhaseId(string phase)
		{
			int? type_id = null;
			switch (phase.ToLower())
			{
				case "n/a": type_id = 100; break;
				case "not applicable": type_id = 100; break;
				case "early phase 1": type_id = 105; break;
				case "phase 1": type_id = 110; break;
				case "phase 1/phase 2": type_id = 115; break;
				case "phase 2": type_id = 120; break;
				case "phase 2/phase 3": type_id = 125; break;
				case "phase 3": type_id = 130; break;
				case "phase 4": type_id = 135; break;
				case "not provided": type_id = 140; break;
			}
			return type_id;
		}

		int? GetPrimaryPurposeId(string primary_purpose)
		{
			int? type_id = null;
			switch (primary_purpose.ToLower())
			{
				case "treatment": type_id = 400; break;
				case "prevention": type_id = 405; break;
				case "diagnostic": type_id = 410; break;
				case "supportive care": type_id = 415; break;
				case "screening": type_id = 420; break;
				case "health services research": type_id = 425; break;
				case "basic science": type_id = 430; break;
				case "device feasibility": type_id = 435; break;
				case "other": type_id = 440; break;
				case "not provided": type_id = 445; break;
				case "educational/counseling/training": type_id = 450; break;
			}
			return type_id;
		}

		int? GetAllocationTypeId(string allocation_type)
		{
			int? type_id = null;
			switch (allocation_type.ToLower())
			{
				case "n/a": type_id = 200; break;
				case "randomized": type_id = 205; break;
				case "non-randomized": type_id = 210; break;
				case "not provided": type_id = 215; break;
			}
			return type_id;
		}

		int? GetDesignTypeId(string design_type)
		{
			int? type_id = null;
			switch (design_type.ToLower())
			{
				case "single group assignment": type_id = 300; break;
				case "parallel assignment": type_id = 305; break;
				case "crossover assignment": type_id = 310; break;
				case "factorial assignment": type_id = 315; break;
				case "sequential assignment": type_id = 320; break;
				case "not provided": type_id = 325; break;
			}
			return type_id;
		}

		int? GetMaskingTypeId(string masking_type)
		{
			int? type_id = null;
			switch (masking_type.ToLower())
			{
				case "none (open label)": type_id = 500; break;
				case "single": type_id = 505; break;
				case "double": type_id = 510; break;
				case "triple": type_id = 515; break;
				case "quadruple": type_id = 520; break;
				case "not provided": type_id = 525; break;
			}
			return type_id;
		}

		int? GetObsModelTypeId(string obsmodel_type)
		{
			int? type_id = null;
			switch (obsmodel_type.ToLower())
			{
				case "cohort": type_id = 600; break;
				case "case control": type_id = 605; break;
				case "case-control": type_id = 605; break;
				case "case-only": type_id = 610; break;
				case "case-crossover": type_id = 615; break;
				case "ecologic or community": type_id = 620; break;
				case "family-based": type_id = 625; break;
				case "other": type_id = 630; break;
				case "not provided": type_id = 635; break;
				case "defined population": type_id = 640; break;
				case "natural history": type_id = 645; break;
			}
			return type_id;
		}

		int? GetTimePerspectiveId(string time_perspective)
		{
			int? type_id = null;
			switch (time_perspective.ToLower())
			{
				case "retrospective": type_id = 700; break;
				case "prospective": type_id = 705; break;
				case "cross-sectional": type_id = 710; break;
				case "other": type_id = 715; break;
				case "not provided": type_id = 720; break;
				case "retrospective/prospective": type_id = 725; break;
				case "longitudinal": type_id = 730; break;
			}
			return type_id;
		}

		int? GetSpecimentRetentionId(string specimen_retention)
		{
			int? type_id = null;
			switch (specimen_retention.ToLower())
			{
				case "none retained": type_id = 800; break;
				case "samples with dna": type_id = 805; break;
				case "samples without dna": type_id = 810; break;
				case "not provided": type_id = 815; break;
			}
			return type_id;
		}


		string TidyPunctuation(string in_name)
		{
			string name = in_name;
			if (name != null)
			{
				if (name.Contains("."))
				{
					// protect these exceptions to the remove full stop rule
					name = name.Replace(".com", "|com");
					name = name.Replace(".gov", "|gov");
					name = name.Replace(".org", "|org");

					name = name.Replace(".", "");

					name = name.Replace("|com", ".com");
					name = name.Replace("|gov", ".gov");
					name = name.Replace("|org", ".org");
				}

				// do this as a loop as there may be several apostrophes that
				// need replacing to different types of quote
				while (name.Contains("'"))
				{
					name = ReplaceApos(name);
				}
			}
			return name;
		}

		string ReplaceApos(string apos_name)
		{
			int apos_pos = apos_name.IndexOf("'");
			int alen = apos_name.Length;
			if (apos_pos != -1)
			{ 
				if (apos_pos == 0)
				{
					apos_name = "‘" + apos_name.Substring(1);
				}
				else if (apos_pos == alen - 1)
				{
					apos_name = apos_name.Substring(0, alen - 1) + "’";
				}
				{
					if (apos_name[apos_pos - 1] == ' ' || apos_name[apos_pos - 1] == '(')
					{
						apos_name = apos_name.Substring(0, apos_pos) + "‘" + apos_name.Substring(apos_pos + 1, alen - apos_pos - 1);

					}
					else
					{
						apos_name = apos_name.Substring(0, apos_pos) + "’" + apos_name.Substring(apos_pos + 1, alen - apos_pos - 1);
					}
				}
			}
			return apos_name;
		}


		string TidyName(string in_name)
		{

			string name = in_name.Replace(".", "");
			string low_name = name.ToLower();

			if (low_name.StartsWith("professor "))
			{
				name = name.Substring(4, name.Length - 10);
				low_name = name.ToLower();
			}
			else if (low_name.StartsWith("prof "))
			{
				name = name.Substring(5, name.Length - 5);
				low_name = name.ToLower();
			}

			if (low_name.StartsWith("dr ")) { name = name.Substring(3, name.Length - 3); }
			else if (low_name.StartsWith("dr med ")) { name = name.Substring(7, name.Length - 7); }

			int comma_pos = name.IndexOf(',');
			if (comma_pos > -1) { name = name.Substring(0, comma_pos); }

			return name;
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


	public class SplitDate
	{
		public int? year;
		public int? month;
		public int? day;
		public string date_string;

		public SplitDate(int? _year, int? _month, int? _day, string _date_string)
		{
			year = _year;
			month = _month;
			day = _day;
			date_string = _date_string;
		}
	}


	public enum MonthsFull
	{
		January = 1, February, March, April, May, June,
		July, August, September, October, November, December
	};


	public enum Months3
	{
		Jan = 1, Feb, Mar, Apr, May, Jun,
		Jul, Aug, Sep, Oct, Nov, Dec
	};


	public class URLChecker
	{
		HttpClient Client = new HttpClient();
		DateTime today = DateTime.Today;

		public async Task CheckURLsAsync(List<DataObjectInstance> web_resources)
		{
			foreach (DataObjectInstance i in web_resources)
			{
				if (i.resource_type_id == 11)  // just do the study docs for now
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
