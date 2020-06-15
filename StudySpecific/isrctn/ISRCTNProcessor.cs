﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace DataHarvester.isrctn
{
	public class ISRCTNProcessor
	{
		URLChecker checker;
		HtmlHelperFunctions hhp;
		HelperFunctions hf;

		public ISRCTNProcessor()
		{
			checker = new URLChecker(); 
			hhp = new HtmlHelperFunctions();
			hf = new HelperFunctions();
		}


		public async Task<Study> ProcessDataAsync(ISCTRN_Record fs, DateTime? download_datetime, DataLayer common_repo)
		{
			Study s = new Study();

			List<StudyIdentifier> identifiers = new List<StudyIdentifier>();
			List<StudyTitle> titles = new List<StudyTitle>();
			List<StudyContributor> contributors = new List<StudyContributor>();
			List<StudyReference> references = new List<StudyReference>();
			List<StudyTopic> topics = new List<StudyTopic>();
			List<StudyFeature> features = new List<StudyFeature>();
			List<DataObject> data_objects = new List<DataObject>();
			List<DataObjectTitle> object_titles = new List<DataObjectTitle>();
			List<DataObjectDate> object_dates = new List<DataObjectDate>();
			List<DataObjectInstance> object_instances = new List<DataObjectInstance>();
			List<StudyLink> studylinks = new List<StudyLink>();

			//List<AvailableIPD> ipd_info = new List<AvailableIPD>();
			//List<StudyRelationship> relationships = new List<StudyRelationship>();

			// get basic study attributes
			string sid = fs.isctrn_id;
			s.sd_sid = sid;
			s.display_title = fs.study_name;   // = public title, default

			titles.Add(new StudyTitle(sid, s.display_title, 15, "Public title", true));

			// study status from trial_status and recruitment_status
			// record for now and see what is available
			string trial_status = fs.trial_status;
			string recruitment_status = fs.recruitment_status;
			s.study_status = trial_status + " :: recruitment :: " + recruitment_status;

			switch (trial_status)
			{
				case "Completed":
					{
						s.study_status = "Completed";
						s.study_status_id = 21; 
						break;
					}
				case "Suspended":
					{
						s.study_status = "Suspended";
						s.study_status_id = 18;
                        break;
					}
				case "Stopped":
					{
						s.study_status = "Terminated";
						s.study_status_id = 22; 
						break;
					}
				case "Ongoing":
					{
						switch (recruitment_status)
						{
							case "Not yet recruiting":
								{
									s.study_status = "Not yet recruiting";
									s.study_status_id = 16;
									break;
								}
							case "Recruiting":
								{
									s.study_status = "Recruiting";
									s.study_status_id = 14;
									break;
								}
							case "No longer recruiting":
								{
									s.study_status = "Active, not recruiting";
									s.study_status_id = 15;
									break;
								}
							case "Suspended":
								{
									s.study_status = "Suspended";
									s.study_status_id = 18;
									break;
								}
						}
						break;
					}
			}

			string conditions = fs.condition_category;
			if (conditions.Contains("'"))
			{
				// add some topics
				string[] conds = conditions.Split(',');
				for (int i = 0; i < conds.Length; i++)
				{
					topics.Add(new StudyTopic(sid, 13, "condition", conds[i], "", "condition_category"));
				}
			}
			else
			{
				// add a single topic
				topics.Add(new StudyTopic(sid, 13, "condition", conditions, "", "condition_category"));
			}

			// study registry entry dates
			string date_assigned_as_string = "";
			if (fs.date_assigned != null)
			{
				DateTime date_assigned = (DateTime)fs.date_assigned;
				date_assigned_as_string = date_assigned.Year.ToString() + " "
								  + ((Months3)date_assigned.Month).ToString() + " " + date_assigned.Day.ToString();
			}

			string date_last_edited_as_string = "";
			if (fs.last_edited != null)
			{
				DateTime last_edit = (DateTime)fs.last_edited;
				date_last_edited_as_string = last_edit.Year.ToString() + " "
								  + ((Months3)last_edit.Month).ToString() + " " + last_edit.Day.ToString();
			}


			// study sponsors
			if (fs.sponsor.Count > 0)
			{
				StudyContributor c = null;
				foreach (Item i in fs.sponsor)
				{
					string id_value = i.item_value;
					switch (i.item_name)
					{
						case "Organisation":
							{
								if (c != null)
								{
									contributors.Add(c);
								}
								c = new StudyContributor(sid, 54, "Trial Sponsor", null, i.item_value, null, null);
								break;
							}
						case "Sponsor type":
							{
								// Ignore...
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "sponsor: " + i.item_name, i.item_value));
								break;
							}

					}
				}

				// do not forget the last contributor
				if (c != null)
				{
					contributors.Add(c);
				}
			}

			string study_sponsor = "";
			if (contributors.Count > 0)
			{
				study_sponsor = contributors[0].organisation_name;
			}


			//public List<Item> contacts { get; set; }
			if (fs.contacts.Count > 0)
			{
				//fs.contacts.Sort();
				StudyContributor c = null; 				
				foreach (Item i in fs.contacts)
				{

					switch (i.item_name)
					{
						case "Type":
							{
								// starts a new contact record...
								// also need to store any pre-existing record
								if (c != null)
								{
									contributors.Add(c);
								}
								c = new StudyContributor(sid, null, null, null, null, null, null);
								if (i.item_value == "Scientific")
								{
									c.contrib_type_id = 51;
									c.contrib_type = "Study Lead";
								}
								else if (i.item_value == "Public")
								{
									c.contrib_type_id = 56;
									c.contrib_type = "Public contact";
								}
								else
								{
									c.contrib_type_id = 0;
									c.contrib_type = i.item_value;
								}
								break;
							}
						case "Primary contact":
							{
								c.person_full_name = i.item_value;
								break;
							}
						case "Additional contact":
							{
								c.person_full_name = i.item_value;
								break;
							}
						case "ORCID ID":
							{
								string orcid;
								if (i.item_value.Contains("/"))
								{
									orcid = i.item_value.Substring(i.item_value.LastIndexOf("/") + 1);
								}
								else
								{
									orcid = i.item_value;
								}
								c.person_identifier = orcid;
								break;
							}
					    case "email_address":
							{
								// ignore...
								break;
							}
						
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "contacts: " + i.item_name, i.item_value));
								break;
							}

					}
				}
				
				// do not forget the last contributor
				if (c != null)
				{
					contributors.Add(c);
				}
			}


			//public List<Item> funders { get; set; }
			if (fs.funders.Count > 0)
			{
				//fs.funders.Sort();
				foreach (Item i in fs.funders)
				{
					string id_value = i.item_value;
					switch (i.item_name)
					{
						case "Funder type":
							{
								// do nothing
								break;
							}
						case "Funder name":
							{
								// check a funder is not simply the sponsor...
								if (i.item_value != study_sponsor)
								{
									contributors.Add(new StudyContributor(sid, 58, "Study Funder", null, i.item_value, null, null));
								}
								break;
							}
						case "Alternative name(s)":
							{
								// do nothing
								break;
							}
						case "Funding Body Type":
							{
								// do nothing
								break;
							}
						case "Funding Body Subtype":
							{
								// do nothing
								break;
							}
						case "Location":
							{
								// do nothing
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "funder: " + i.item_name, i.item_value));
								break;
							}
					}
				}
			}

			// study identifiers
			// do the isrctn id first...
			string reg_id_date = fs.date_assigned?.ToString("dd mmm yyyy");
			identifiers.Add(new StudyIdentifier(sid, fs.isctrn_id, 11, "Trial Registry ID", 100126, "ISRCTN", date_assigned_as_string, null));

			// then any others that might be listed
			if (fs.identifiers.Count > 0)
			{
				foreach (Item i in fs.identifiers)
				{
					switch (i.item_name)
					{
						case "Protocol/serial number":
							{
								identifiers.Add(new StudyIdentifier(sid, i.item_value, 14, "Sponsor ID", 0, study_sponsor, null, null));
								break;
							}
						case "ClinicalTrials.gov number":
							{
								identifiers.Add(new StudyIdentifier(sid, i.item_value, 11, "Trial Registry ID", 100120, "ClinicalTrials.gov", null, null));
								break;
							}
						case "EudraCT number":
							{
								identifiers.Add(new StudyIdentifier(sid, i.item_value, 11, "Trial Registry ID", 100123, "EU CTR", null, null));
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "identifier: " + i.item_name, i.item_value));
								break;
							}

					}
				}
			}


			// design info from List<Item> study_info { get; set; }
			string PIS_details = "";
			if (fs.study_info.Count > 0)
			{
				//fs.study_info.Sort();
				foreach (Item i in fs.study_info)
				{
					switch (i.item_name)
					{
						case "Scientific title":
							{
								titles.Add(new StudyTitle(sid, i.item_value, 16, "Trial Registry title", false));
								break;
							}
						case "Acronym":
							{
								titles.Add(new StudyTitle(sid, i.item_value, 14, "Acronym or Abbreviation", false));
								break;
							}
						case "Study hypothesis":
							{
								if (!i.item_value.StartsWith("Study"))
								{
									s.brief_description += "Study hypothesis: ";
								}
								s.brief_description += i.item_value;
								break;
							}
						case "Primary study design":
							{
								if (i.item_value == "Interventional")
								{
									s.study_type = "Interventional";
									s.study_type_id = 11;
								}
								else if (i.item_value == "Observational")
								{
									s.study_type = "Observational";
									s.study_type_id = 12;
								}
								else if (i.item_value == "Other")
								{
									s.study_type = "Other";
									s.study_type_id = 16;
								}
								break;
							}
						case "Secondary study design":
							{
								string design = i.item_value.ToLower().Replace("randomized", "randomised");
								string design2 = design.Replace("non randomised", "non-randomised");
								if (design2.Contains("non-randomised"))
								{
									features.Add(new StudyFeature(sid, 22, "allocation type", 210, "Nonrandomised"));
								}
								else if (design2.Contains("randomised"))
								{
									features.Add(new StudyFeature(sid, 22, "allocation type", 205, "Randomised"));
								}
								else
								{
									features.Add(new StudyFeature(sid, 22, "allocation type", 215, "Not provided"));
								}
								break;
							}
						case "Trial type":
							{
								int value_id = 0;
								string value_name = "";
								switch (i.item_value)
								{
									case "Treatment":
										{
											value_id = 400; value_name = "Treatment";
											break;
										}
									case "Prevention":
										{
											value_id = 405; value_name = "Prevention";
											break;
										}
									case "Quality of life":
										{
											value_id = 440; value_name = "Other";
											break;
										}
									case "Other":
										{
											value_id = 440; value_name = "Other";
											break;
										}
									case "Not Specified":
										{
											value_id = 445; value_name = "Not provided";
											break;
										}
									case "Diagnostic":
										{
											value_id = 410; value_name = "Diagnostic";
											break;
										}
									case "Screening":
										{
											value_id = 420; value_name = "Screening";
											break;
										}
								}
								features.Add(new StudyFeature(sid, 21, "primary purpose", value_id, value_name));
								break;
							}
						case "Study design":
							{
								string design = i.item_value.ToLower().Replace("open label", "open-label").Replace("single blind", "single-blind");
								string design2 = design.Replace("double blind", "double-blind").Replace("triple blind", "triple-blind").Replace("quadruple blind", "quadruple-blind");
								
								if (design2.Contains("open-label"))
								{
									features.Add(new StudyFeature(sid, 24, "masking", 500, "None (Open Label)"));
								}
								else if (design2.Contains("single-blind"))
								{
									features.Add(new StudyFeature(sid, 24, "masking", 505, "Single"));
								}
								else if (design2.Contains("double-blind"))
								{
									features.Add(new StudyFeature(sid, 24, "masking", 510, "Double"));
								}
								else if (design2.Contains("triple-blind"))
								{
									features.Add(new StudyFeature(sid, 24, "masking", 515, "Triple"));
								}
								else if (design2.Contains("quadruple-blind"))
								{
									features.Add(new StudyFeature(sid, 24, "masking", 520, "Quadruple"));
								}
								else
								{
									features.Add(new StudyFeature(sid, 24, "masking", 525, "Not provided"));
								}

								string design3 = design2.Replace("case control", "case-control");

								if (design3.Contains("cohort"))
								{
									features.Add(new StudyFeature(sid, 30, "masking", 600, "Cohort"));
								}
								else if (design3.Contains("case-control"))
								{
									features.Add(new StudyFeature(sid, 30, "masking", 605, "Case-Control"));
								}
								else if (design3.Contains("cross section"))
								{
									features.Add(new StudyFeature(sid, 31, "masking", 710, "Cross-sectional"));
								}
								else if (design3.Contains("longitudinal"))
								{
									features.Add(new StudyFeature(sid, 31, "masking", 730, "Longitudinal"));
								}

								break;
							}
						case "Patient information sheet":
							{
								if (!i.item_value.StartsWith("Not available") && !i.item_value.StartsWith("Not applicable"))
								{
									if (i.item_value.Contains("<a href"))
									{
										// try and create a data object later corresponding to the PIS (object and instance only)
										PIS_details = i.item_value;
									}
								}
								break;
							}
						case "Condition":
							{
								topics.Add(new StudyTopic(sid, 13, "condition", i.item_value, "", "study_info - condition"));
								break;
							}
						case "Drug names":
							{
								topics.Add(new StudyTopic(sid, 12, "chemical / agent", i.item_value, "", "study_info - drug names"));
								break;
							}
						case "Phase":
							{
								int value_id = 0;
								string value_name = "";
         						switch (i.item_value)
								{
									case "Phase I":
										{
											value_id = 110; value_name = "Phase 1";
											break;
										}
									case "Phase I/II":
										{
											value_id = 115; value_name = "Phase 1/Phase 2";
											break;
										}
									case "Phase II":
										{
											value_id = 120; value_name = "Phase 2";
											break;
										}
									case "Phase II/III":
										{
											value_id = 125; value_name = "Phase 2/Phase 3";
											break;
										}
									case "Phase III":
										{
											value_id = 130; value_name = "Phase 3";
											break;
										}
									case "Phase III/IV":
										{
											value_id = 130; value_name = "Phase 3";
											break;
										}
									case "Phase IV":
										{
											value_id = 135; value_name = "Phase 4";
											break;
										}
									case "Not Specified":
										{
											value_id = 140; value_name = "Not provided";
											break;
										}
								}
								features.Add(new StudyFeature(sid, 20, "phase", value_id, value_name));
								break;
							}
						case "Primary outcome measure":
							{
								if (i.item_value != "Not provided at time of registration")
								{
									if (i.item_value.StartsWith("Primary"))
									{
										s.brief_description += " " + i.item_value;
									}
									else
									{
										s.brief_description += " Primary outcome measures: " + i.item_value;
									}
								}
								break;
							}
						case "Overall trial start date":
							{
								if (i.item_value != "Not provided at time of registration")
								{
									CultureInfo culture = new CultureInfo("en-UK", false);
									if (DateTime.TryParse(i.item_value, culture, DateTimeStyles.AssumeLocal, out DateTime start_date))
									{
										s.study_start_year = start_date.Year;
										s.study_start_month = start_date.Month;
									}
								}
								break;
							}
						case "Reason abandoned (if study stopped)":
							{
								if (i.item_value != "Not provided at time of registration")
								{
									s.brief_description += " Reason study stopped: " + i.item_value;
								}
								break;
							}
						case "Overall trial end date":
							{
								// do nothing for now
								break;
							}
						case "Intervention type":
							{
								// do nothing for now
								break;
							}
						case "Trial setting":
							{
								// do nothing for now
								break;
							}
						case "Ethics approval":
							{
								// do nothing for now
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "study_info: " + i.item_name, i.item_value));
								break;
							}

					}
				}
			}

			// eligibility from List<Item> eligibility { get; set; }
			if (fs.eligibility.Count > 0)
			{
				//fs.eligibility.Sort();
				foreach (Item i in fs.eligibility)
				{
					switch (i.item_name)
					{
						case "Age group":
							{
								switch (i.item_value)
								{
									case "Adult":
										{
											s.min_age = 18;
											s.min_age_units = "Years";
											s.min_age_units_id = 17;
											s.max_age = 65;
											s.max_age_units = "Years";
											s.max_age_units_id = 17;
											break;
										}
									case "Senior":
										{
											s.min_age = 66;
											s.min_age_units = "Years";
											s.min_age_units_id = 17;
											break;
										}
									case "Neonate":
										{
											s.max_age = 28;
											s.max_age_units = "Days";
											s.max_age_units_id = 14;
											break;
										}
									case "Child":
										{
											s.min_age = 29;
											s.min_age_units = "Days";
											s.min_age_units_id = 14;
											s.max_age = 17;
											s.max_age_units = "Years";
											s.max_age_units_id = 17;
											break;
										}
									default:
										{
											break;
										}
								}
							}
							break;

						case "Gender":
							{
								s.study_gender_elig = i.item_value;
								switch (s.study_gender_elig)
								{
									case "Both":
										{
											s.study_gender_elig = "All";
											s.study_gender_elig_id = 900;
											break;
										}
									case "Female":
										{
											s.study_gender_elig_id = 905;
											break;
										}
									case "Male":
										{
											s.study_gender_elig_id = 910;
											break;
										}
								}
								break;
							}
						case "Target number of participants":
							{
								if (Int32.TryParse(i.item_value, out int enrollment))
								{
									s.study_enrolment = enrollment;
								}
								break;
							}
						case "Total final enrolment":
							{
								// if available replace with this...
								if (Int32.TryParse(i.item_value, out int enrollment))
								{
									s.study_enrolment = enrollment;
								}
								break;
							}
						case "Recruitment start date":
							{
								// do nothing for now
								break;
							}
						case "Recruitment end date":
							{
								// do nothing for now
								break;
							}
						case "Participant type":
							{
								// do nothing for now
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "eligibility: " + i.item_name, i.item_value));
								break;
							}

					}
				}
			}


			// DATA OBJECTS and their attributes
     		// initial data object is the ISRCTN registry entry

			int pub_year = 0;
			if (fs.date_assigned != null)
			{
				pub_year = ((DateTime)fs.date_assigned).Year;
			}
			string object_display_title = s.display_title + " :: ISRCTN registry entry";

			// create hash Id for the data object
			string sd_oid = hf.CreateMD5(sid + object_display_title);

			DataObject d = new DataObject(sd_oid, sid, object_display_title, pub_year,
				  23, "Text", 13, "Trial Registry entry", 100126, "ISRCTN", 12, download_datetime);
			d.doi = fs.doi;
			d.doi_status_id = 1;
			data_objects.Add(d);

			// data object title is the single display title...
			object_titles.Add(new DataObjectTitle(sd_oid, object_display_title, 
				                                     22, "Study short name :: object type", true));
			if (fs.last_edited != null)
			{
				DateTime last_edit = (DateTime)fs.last_edited;
				object_dates.Add(new DataObjectDate(sd_oid, 18, "Updated", 
					              last_edit.Year, last_edit.Month, last_edit.Day, date_last_edited_as_string));
			}

			if (fs.date_assigned != null)
			{
				DateTime date_assigned = (DateTime)fs.date_assigned;
				object_dates.Add(new DataObjectDate(sd_oid, 15, "Created",
								  date_assigned.Year, date_assigned.Month, date_assigned.Day, date_assigned_as_string));
			}

			// instance url can be derived from the ISRCTN number
			object_instances.Add(new DataObjectInstance(sd_oid, 100126, "ISRCTN", 
						"https://www.isrctn.com/" + sid, true, 35, "Web text"));

			// is there a PIS available
			if (PIS_details != "")
			{
				// PIS note includes an href to a web address
				int ref_start = PIS_details.IndexOf("href=") + 6;
				int ref_end = PIS_details.IndexOf("\"", ref_start + 1);
				string href = PIS_details.Substring(ref_start, ref_end - ref_start);

				// first check link does not provide a 404
				if (await checker.CheckURLAsync(href))
				{
					int res_type_id = 35;
					string res_type = "Web text";
					if (href.ToLower().EndsWith("pdf"))
					{
						res_type_id = 11;
						res_type = "PDF";
					}
					else if (href.ToLower().EndsWith("docx") || href.ToLower().EndsWith("doc"))
					{
						res_type_id = 16;
						res_type = "Word doc";
					}

					object_display_title = s.display_title + " :: patient information sheet";
					sd_oid = hf.CreateMD5(sid + object_display_title);

					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
					  23, "Text", 19, "Patient information sheets", null, study_sponsor, 12, download_datetime));
					object_titles.Add(new DataObjectTitle(sd_oid, object_display_title,
														 22, "Study short name :: object type", true));
					DataObjectInstance instance = new DataObjectInstance(sd_oid, null, "",
							href, true, res_type_id, res_type);
					instance.url_last_checked = DateTime.Today;
    				object_instances.Add(instance);
				}

			}


			// possible reference / publications
			if (fs.publications.Count > 0)
			{
				//fs.publications.Sort();
				foreach (Item i in fs.publications)
				{
					switch (i.item_name)
					{
						case "Publication and dissemination plan":
							{
								if (i.item_value != "Not provided at time of registration")
								{
									s.data_sharing_statement = "General: " + i.item_value;
								}
								break;
							}
						case "Participant level data":
							{
								if (i.item_value != "Not provided at time of registration")
								{
									s.data_sharing_statement += " IPD: " + i.item_value;
								}
								break;
							}
						case "Intention to publish date":
							{
								// do nothing for now
								break;
							}
						case "Publication list":
							{
								// formats vary but broadly like "yyyy Results in <a...
								string itemline = i.item_value.Replace("<br>", "").Replace("<br/>", "");
								string refs = itemline.Replace("<a", "||").Replace("</a>", "||").Replace(">", "||");
								string[] ref_items = refs.Split("||");
								for (int j = 0; j < ref_items.Length; j+=3)
								{
									// need to avoid trying to use any 'odd' item on the end
									if (j < ref_items.Length - 2)
									{
										string link = ref_items[j + 2].Trim().ToLower();
										int pmid = 0;
										bool pmid_found = false;
										if (link.Contains("pubmed"))
										{
											if (link.Contains("list_uids="))
											{
												string poss_pmid = link.Substring(link.IndexOf("list_uids=") + 10);
												if (Int32.TryParse(poss_pmid, out pmid))
												{
													pmid_found = true;
												}
											}
											else if (link.Contains("termtosearch="))
											{
												string poss_pmid = link.Substring(link.IndexOf("termtosearch=") + 13);
												if (Int32.TryParse(poss_pmid, out pmid))
												{
													pmid_found = true;
												}
											}
											else if (link.Contains("term="))
											{
												string poss_pmid = link.Substring(link.IndexOf("term=") + 5);
												if (Int32.TryParse(poss_pmid, out pmid))
												{
													pmid_found = true;
												}
											}
											else 
											{
												// 'just' pubmed...
												string poss_pmid = link.Substring(link.LastIndexOf("/") + 1);
												if (Int32.TryParse(poss_pmid, out pmid))
												{
													pmid_found = true;
												}
											}

											if (pmid_found && pmid > 0)
											{
												references.Add(new StudyReference(sid, ref_items[j], ref_items[j + 2], pmid.ToString(), null));

											}
											else
											{
												references.Add(new StudyReference(sid, ref_items[j], ref_items[j + 2], null, null));

											}
										}
									}
								}
								break;
							}
						case "Basic results (scientific)":
							{
								// formats vary broadly - may need to be sorted afterwards 
								// many refer to results pages in EUCTR and / or ClinicalTrials.gov
								// could assume that these at least will be covered in any case...
								string refs = i.item_value.Replace("<a", "||").Replace("</a>", "||").Replace(">", "||");
								string[] ref_items = refs.Split("||");
								if (ref_items.Length > 1)
								{
									for (int j = 0; j < ref_items.Length; j++)
									{
										string ref_item = ref_items[j].Trim().ToLower();
										if (ref_item.StartsWith("http") && ref_item.Contains("servier"))
										{
											//references.Add(new Reference(sd_id, "result ref", ref_items[j], null));
											string object_type;
											int object_type_id;

											// N.B. a few documents categorised manually as their name includes no 
											// clue to their content...
											if (ref_item.Contains("synopsis"))
											{
												object_type = "CSR Summary";
												object_type_id = 79;
											}
											else if (ref_item.Contains("lay-summary"))
											{
												object_type = "Study Overview";
												object_type_id = 38;
											}
											else 
											{
												object_type = "Other";
												object_type_id = 37;
											}

											int res_type_id = 0;
											string res_type = "Not yet known";
											if (ref_item.ToLower().EndsWith("pdf"))
											{
												res_type_id = 11;
												res_type = "PDF";
											}
											else if (ref_item.ToLower().EndsWith("docx") || ref_item.ToLower().EndsWith("doc"))
											{
												res_type_id = 16;
												res_type = "Word doc";
											}

											object_display_title = s.display_title + " :: " + object_type;
											sd_oid = hf.CreateMD5(sid + object_display_title);

											data_objects.Add(new DataObject(sd_oid, sid, s.display_title + " :: " + object_type, s.study_start_year, 
														23, "Text", object_type_id, object_type, 
														101418, "Servier", 11, download_datetime));
											object_titles.Add(new DataObjectTitle(sd_oid, s.display_title + " :: " + object_type,
														22, "Study short name :: object type", true));
											object_instances.Add(new DataObjectInstance(sd_oid,  101418, "Servier",
													ref_item, true, res_type_id, res_type));

										}

									}
								}
								break;
							}
						default:
							{
								studylinks.Add(new
									StudyLink(sid, "publications: " + i.item_name, i.item_value));
								break;
							}
					}
				}
			}

			// possible additional files
			// public List<Item> additional_files { get; set; }
			if (fs.additional_files.Count > 0)
			{
				//fs.additional_files.Sort();
				foreach (Item i in fs.additional_files)
				{
					// need to correct an extraction error here...
					string link = i.item_value.Replace("//editorial", "/editorial");

					// create object details
					string object_type, test_name;
					int object_type_id;
					test_name = i.item_name.ToLower();

					// N.B. a few documents categorised manually as their name includes no 
					// clue to their content...
					if (test_name.Contains("results"))
					{
						object_type = "Unpublished Study Report";
						object_type_id = 85;
					}
					else if (test_name.Contains("protocol")
									  || i.item_name == "ISRCTN23416732_v5_13June2018.pdf"
						              || i.item_name == "ISRCTN36746902 _V2.2_final_30Jan20.pdf"
						              || i.item_name == "ISRCTN84288963_v8.0_21062018.docx.pdf")
					{
						object_type = "Study Protocol";
						object_type_id = 11;
					}
					else if (test_name.Contains("pis") || test_name.Contains("participant")
							   || i.item_name == "ISRCTN88166769.pdf")
					{ 
						object_type = "Patient information sheets";
						object_type_id = 19;
					}
					else if (test_name.Contains("sap") || test_name.Contains("statistical") 
								|| i.item_name == "ISRCTN14148239_V1.0_21Oct19.pdf")
					{
						object_type = "Statistical analysis plan";
						object_type_id = 22;
					}
					else if
						(test_name.Contains("consent"))
					{
						object_type = "Informed consent forms";
						object_type_id = 18;
					}
					else 
					{
						object_type = "Other";
						object_type_id = 37;
					}

					int res_type_id = 0;
					string res_type = "Not yet known";
					if (i.item_name.ToLower().EndsWith("pdf"))
					{
						res_type_id = 11;
						res_type = "PDF";
					}
					else if (i.item_name.ToLower().EndsWith("docx") || i.item_name.ToLower().EndsWith("doc"))
					{
						res_type_id = 16;
						res_type = "Word doc";
					}
					else if (i.item_name.ToLower().EndsWith("pptx") || i.item_name.ToLower().EndsWith("ppt"))
					{
						res_type_id = 20;
						res_type = "PowerPoint";
					}

					object_display_title = i.item_name;
					sd_oid = hf.CreateMD5(sid + object_display_title);

					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
								23, "Text", object_type_id, object_type, 100126, "ISRCTN", 11, download_datetime));
					object_titles.Add(new DataObjectTitle(sd_oid, object_display_title,
								20, "Unique data object title", true));
					object_instances.Add(new DataObjectInstance(sd_oid, 100126, "ISRCTN",
							link, true, res_type_id, res_type));
					break;

				}
			}


			// possible object of a trial web site if one exists for this study
			if (!string.IsNullOrEmpty(fs.trial_website))
			{
				// first check website link does not provide a 404
				if (await checker.CheckURLAsync(fs.trial_website))
				{
					object_display_title = s.display_title + " :: website";
					sd_oid = hf.CreateMD5(sid + object_display_title);

					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
							23, "Text", 134, "Website", null, study_sponsor, 12, download_datetime));
					object_titles.Add(new DataObjectTitle(sd_oid, object_display_title, 
														 22, "Study short name :: object type", true));
					DataObjectInstance instance = new DataObjectInstance(sd_oid, null, study_sponsor,
							fs.trial_website, true, 35, "Web text");
					instance.url_last_checked = DateTime.Today;
					object_instances.Add(instance);
				}
			}


			s.identifiers = identifiers;
			s.titles = titles;
			s.contributors = contributors;
			s.references = references;
			//s.ipd_info = ipd_info;
			s.topics = topics;
			s.features = features;
			//s.relationships = relationships;
			s.studylinks = studylinks;

			s.data_objects = data_objects;
			s.object_titles = object_titles;
			s.object_dates = object_dates;
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

			//if (s.ipd_info.Count > 0) r.StoreIPDInfo(m.ipd_copyhelper, s.ipd_info);

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
				catch(Exception e)
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
