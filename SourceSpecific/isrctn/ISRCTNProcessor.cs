using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace DataHarvester.isrctn
{
	public class ISRCTNProcessor
	{
		//URLChecker checker;

		public ISRCTNProcessor()
		{
			//checker = new URLChecker(); 
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
			//List<StudyLink> studylinks = new List<StudyLink>();

			List<DataObject> data_objects = new List<DataObject>();
			List<ObjectTitle> object_titles = new List<ObjectTitle>();
			List<ObjectDate> object_dates = new List<ObjectDate>();
			List<ObjectInstance> object_instances = new List<ObjectInstance>();
			

			//List<AvailableIPD> ipd_info = new List<AvailableIPD>();
			//List<StudyRelationship> relationships = new List<StudyRelationship>();

			// get basic study attributes
			string sid = fs.isctrn_id;
			s.sd_sid = sid;
			s.display_title = fs.study_name;   // = public title, default
			s.datetime_of_data_fetch = download_datetime;

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
					topics.Add(new StudyTopic(sid, 13, "condition", conds[i]));
				}
			}
			else
			{
				// add a single topic
				topics.Add(new StudyTopic(sid, 13, "condition", conditions));
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
								if (StringHelpers.FilterOut_Null_OrgNames(i.item_value.ToLower()) != "")
								{
									c = new StudyContributor(sid, 54, "Trial Sponsor", null, StringHelpers.TidyOrgName(i.item_value, sid), null, null);
								}
								break;
							}
						case "Sponsor type":
							{
								// Ignore...
								break;
							}
						default:
							{
								// Ignore...
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
								// Ignore...
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
								if (StringHelpers.FilterOut_Null_OrgNames(i.item_value.ToLower()) != "")
								{
									// check a funder is not simply the sponsor...
									string funder = StringHelpers.TidyOrgName(i.item_value, sid);
									if (funder != study_sponsor)
									{
										contributors.Add(new StudyContributor(sid, 58, "Study Funder", null, funder, null, null));
									}
									
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
								// Ignore...
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
								IdentifierDetails idd;
								if (i.item_value.Contains(";"))
								{
									string[] items = i.item_value.Split(";");
									foreach (string item in items)
									{
										string item2 = item.Trim();
										idd = IdentifierHelpers.GetISRCTNIdentifierProps(item2, study_sponsor);
										if (idd.id_type != "Protocol version")
										{
											identifiers.Add(new StudyIdentifier(sid, idd.id_value, idd.id_type_id, idd.id_type, 
												                                   idd.id_org_id, idd.id_org, null, null));
										}
									}
								}
								else if(i.item_value.Contains(",") && 
									(i.item_value.ToLower().Contains("iras") || i.item_value.ToLower().Contains("hta")))
								{
									string[] items = i.item_value.Split(",");
									foreach (string item in items)
									{
										string item2 = item.Trim();
										idd = IdentifierHelpers.GetISRCTNIdentifierProps(item2, study_sponsor);
										if (idd.id_type != "Protocol version")
										{
											identifiers.Add(new StudyIdentifier(sid, idd.id_value, idd.id_type_id, idd.id_type,
																				   idd.id_org_id, idd.id_org, null, null));
										}
									}
								}
								else
								{
									idd = IdentifierHelpers.GetISRCTNIdentifierProps(i.item_value, study_sponsor);
									if (idd.id_type != "Protocol version")
									{
										identifiers.Add(new StudyIdentifier(sid, idd.id_value, idd.id_type_id, idd.id_type, 
											                                idd.id_org_id, idd.id_org, null, null));
									}
								}
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
								// Ignore...
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
								topics.Add(new StudyTopic(sid, 13, "condition", i.item_value));
								break;
							}
						case "Drug names":
							{
								topics.Add(new StudyTopic(sid, 12, "chemical / agent", i.item_value));
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
								// Ignore...
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
								// Ignore...
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
			string sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

			DataObject d = new DataObject(sd_oid, sid, object_display_title, pub_year,
				  23, "Text", 13, "Trial Registry entry", 100126, "ISRCTN", 12, download_datetime);
			d.doi = fs.doi;
			d.doi_status_id = 1;
			data_objects.Add(d);

			// data object title is the single display title...
			object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 
				                                     22, "Study short name :: object type", true));
			if (fs.last_edited != null)
			{
				DateTime last_edit = (DateTime)fs.last_edited;
				object_dates.Add(new ObjectDate(sd_oid, 18, "Updated", 
					              last_edit.Year, last_edit.Month, last_edit.Day, date_last_edited_as_string));
			}

			if (fs.date_assigned != null)
			{
				DateTime date_assigned = (DateTime)fs.date_assigned;
				object_dates.Add(new ObjectDate(sd_oid, 15, "Created",
								  date_assigned.Year, date_assigned.Month, date_assigned.Day, date_assigned_as_string));
			}

			// instance url can be derived from the ISRCTN number
			object_instances.Add(new ObjectInstance(sd_oid, 100126, "ISRCTN", 
						"https://www.isrctn.com/" + sid, true, 35, "Web text"));

			// is there a PIS available
			if (PIS_details != "")
			{
				// PIS note includes an href to a web address
				int ref_start = PIS_details.IndexOf("href=") + 6;
				int ref_end = PIS_details.IndexOf("\"", ref_start + 1);
				string href = PIS_details.Substring(ref_start, ref_end - ref_start);

				// first check link does not provide a 404
				if (true) //await HtmlHelpers.CheckURLAsync(href))
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
					sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
					  23, "Text", 19, "Patient information sheets", null, study_sponsor, 12, download_datetime));
					object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
														 22, "Study short name :: object type", true));
					ObjectInstance instance = new ObjectInstance(sd_oid, null, "",
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
												references.Add(new StudyReference(sid, pmid.ToString(),ref_items[j + 2], ref_items[j],  null));

											}
											else
											{
												references.Add(new StudyReference(sid, null, ref_items[j + 2], ref_items[j], null));

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
											sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

											data_objects.Add(new DataObject(sd_oid, sid, s.display_title + " :: " + object_type, s.study_start_year, 
														23, "Text", object_type_id, object_type, 
														101418, "Servier", 11, download_datetime));
											object_titles.Add(new ObjectTitle(sd_oid, s.display_title + " :: " + object_type,
														22, "Study short name :: object type", true));
											object_instances.Add(new ObjectInstance(sd_oid,  101418, "Servier",
													ref_item, true, res_type_id, res_type));

										}

									}
								}
								break;
							}
						default:
							{
								// Ignore...
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
					sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
								23, "Text", object_type_id, object_type, 100126, "ISRCTN", 11, download_datetime));
					object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
								20, "Unique data object title", true));
					object_instances.Add(new ObjectInstance(sd_oid, 100126, "ISRCTN",
							link, true, res_type_id, res_type));
					break;

				}
			}


			// possible object of a trial web site if one exists for this study
			if (!string.IsNullOrEmpty(fs.trial_website))
			{
				// first check website link does not provide a 404
				if (true) //await HtmlHelpers.CheckURLAsync(fs.trial_website))
				{
					object_display_title = s.display_title + " :: website";
					sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

					data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
							23, "Text", 134, "Website", null, study_sponsor, 12, download_datetime));
					object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 
														 22, "Study short name :: object type", true));
					ObjectInstance instance = new ObjectInstance(sd_oid, null, study_sponsor,
							fs.trial_website, true, 35, "Web text");
					instance.url_last_checked = DateTime.Today;
					object_instances.Add(instance);
				}
			}


			// Check brief description and 
			// data sharing statement for html
			// after getting rid of any sup / subs, divs and spans.

			s.brief_description = HtmlHelpers.replace_tags(s.brief_description);
			s.bd_contains_html = HtmlHelpers.check_for_tags(s.brief_description);

			s.data_sharing_statement = HtmlHelpers.replace_tags(s.data_sharing_statement);
			s.dss_contains_html = HtmlHelpers.check_for_tags(s.data_sharing_statement);


			s.identifiers = identifiers;
			s.titles = titles;
			s.contributors = contributors;
			s.references = references;
			s.topics = topics;
			s.features = features;
			//s.studylinks = studylinks;

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


			if (s.contributors.Count > 0)
			{
				repo.StoreStudyContributors(StudyCopyHelpers.study_contributors_helper,
										  s.contributors);
			}

			/*
			if (s.studylinks.Count > 0)
			{
				repo.StoreStudyLinks(StudyCopyHelpers.study_links_helper,
										  s.studylinks);
			}
			*/

			if (s.topics.Count > 0)
			{
				repo.StoreStudyTopics(StudyCopyHelpers.study_topics_helper,
										  s.topics);
			}

			if (s.features.Count > 0)
			{
				repo.StoreStudyFeatures(StudyCopyHelpers.study_features_helper,
										  s.features);
			}


			if (s.data_objects.Count > 0)
			{
				repo.StoreDataObjects(ObjectCopyHelpers.data_objects_helper,
										  s.data_objects);
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

			if (s.object_dates.Count > 0)
			{
				repo.StoreObjectDates(ObjectCopyHelpers.object_dates_helper,
										  s.object_dates);
			}

		}

	}


	/*
	public class URLChecker
	{
		HttpClient Client = new HttpClient();
		DateTime today = DateTime.Today;

		public async Task CheckURLsAsync(List<ObjectInstance> web_resources)
		{
			foreach (ObjectInstance i in web_resources)
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
	*/

}
