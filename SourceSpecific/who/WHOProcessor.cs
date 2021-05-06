using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using Serilog;

namespace DataHarvester.who
{
    public class WHOProcessor : IProcessor
    {
        IStorageDataLayer _storage_repo;
        IMonitorDataLayer _mon_repo;
        ILogger _logger;

        public WHOProcessor(IStorageDataLayer storage_repo, IMonitorDataLayer mon_repo, ILogger logger)
        {
            _storage_repo = storage_repo;
            _mon_repo = mon_repo;
            _logger = logger;
        }

        public Study ProcessData(XmlDocument d, DateTime? download_datetime)
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

            StringHelpers sh = new StringHelpers(_logger, _mon_repo);
            DateHelpers dh = new DateHelpers();
            TypeHelpers th = new TypeHelpers();
            MD5Helpers hh = new MD5Helpers();
            HtmlHelpers mh = new HtmlHelpers(_logger);
            IdentifierHelpers ih = new IdentifierHelpers();


            // First convert the XML document to a Linq XML Document.

            XDocument xDoc = XDocument.Load(new XmlNodeReader(d));

            // Obtain the main top level elements of the registry entry.
            // In most cases study will have already been registered in CGT.
            XElement r = xDoc.Root;

            string sid = GetElementAsString(r.Element("sd_sid"));
            s.sd_sid = sid;
            s.datetime_of_data_fetch = download_datetime;

            // transfer features of main study object

            s.sd_sid = sid;
            s.datetime_of_data_fetch = download_datetime;

            string date_registration = GetElementAsString(r.Element("date_registration"));
            int? source_id = GetElementAsInt(r.Element("source_id"));
            string public_title = sh.CheckTitle(GetElementAsString(r.Element("")));
            string scientific_title = sh.CheckTitle(GetElementAsString(r.Element("scientific_title")));

            SplitDate registration_date = null;
            if (!string.IsNullOrEmpty(date_registration))
            {
                registration_date = dh.GetDatePartsFromISOString(date_registration);
            }

            study_identifiers.Add(new StudyIdentifier(sid, sid, 11, "Trial Registry ID", source_id,
                                     ih.get_source_name(source_id), registration_date?.date_string, null));

            // titles
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
            string interventions = GetElementAsString(r.Element("interventions"));
            string primary_outcome = GetElementAsString(r.Element("primary_outcome"));
            string design_string = GetElementAsString(r.Element("design_string"));

            if (!string.IsNullOrEmpty(interventions))
            {
                interventions = interventions.Trim();
                if (!interventions.ToLower().StartsWith("intervention"))
                {
                    interventions = "Interventions: " + interventions;
                }
                if (!interventions.EndsWith(".") && !interventions.EndsWith(";"))
                {
                    interventions += ".";
                }
            }

            if (!string.IsNullOrEmpty(primary_outcome))
            {
                primary_outcome = primary_outcome.Trim();
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

            string study_design = "";
            if (!string.IsNullOrEmpty(design_string)
                && !design_string.ToLower().Contains("not selected"))
            {
                study_design = design_string.Trim();
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
                s.brief_description = mh.replace_tags(s.brief_description);
                s.bd_contains_html = mh.check_for_tags(s.brief_description);
            }

            // data sharing statement
            string ipd_description = GetElementAsString(r.Element("ipd_description"));
            if (!string.IsNullOrEmpty(ipd_description)
                && ipd_description.Length > 10
                && ipd_description.ToLower() != "not available"
                && ipd_description.ToLower() != "not avavilable"
                && ipd_description.ToLower() != "not applicable"
                && !ipd_description.Contains("justification or reason for"))
            {
                s.data_sharing_statement = ipd_description;
                if (s.data_sharing_statement.Contains("<"))
                {
                    s.data_sharing_statement = mh.replace_tags(s.data_sharing_statement);
                    s.dss_contains_html = mh.check_for_tags(s.data_sharing_statement);
                }
            }

            string date_enrolment = GetElementAsString(r.Element("date_enrolment"));
            if (!string.IsNullOrEmpty(date_enrolment))
            {
                SplitDate enrolment_date = dh.GetDatePartsFromISOString(date_enrolment);
                if (enrolment_date?.year > 1960)
                {
                    s.study_start_year = enrolment_date.year;
                    s.study_start_month = enrolment_date.month;
                }
            }


            // study type and status 
            string study_type = GetElementAsString(r.Element("study_type"));
            string study_status = GetElementAsString(r.Element("study_status"));

            if (!string.IsNullOrEmpty(study_type))
            {
                if (study_type.StartsWith("Other"))
                {
                    s.study_type = "Other";
                    s.study_type_id = 16;
                }
                else
                {
                    s.study_type = study_type; ;
                    s.study_type_id = th.GetTypeId(s.study_type);
                }
            }

            if (!string.IsNullOrEmpty(study_status))
            {
                if (study_status.StartsWith("Other"))
                {
                    s.study_status = "Other";
                    s.study_status_id = 24;

                }
                else
                {
                    s.study_status = study_status;
                    s.study_status_id = th.GetStatusId(s.study_status);
                }
            }


            // enrolment targets, gender and age groups
            int? enrolment = 0;

            // use actual enrolment figure if present and not a data or a dummy figure
            string results_actual_enrollment = GetElementAsString(r.Element("results_actual_enrollment"));
            if (!string.IsNullOrEmpty(results_actual_enrollment)
                && !results_actual_enrollment.Contains("9999")
                && !Regex.Match(results_actual_enrollment, @"\d{4}-\d{2}-\d{2}").Success)
            {
                if (Regex.Match(results_actual_enrollment, @"\d+").Success)
                {
                    string enrolment_as_string = Regex.Match(results_actual_enrollment, @"\d+").Value;
                    if (Int32.TryParse(enrolment_as_string, out int numeric_value))
                    {
                        if (numeric_value < 10000)
                        {
                            enrolment = numeric_value;
                        }
                    }
                    else
                    {
                        // what is going on?
                        _logger.Error("Odd enrolment string: " + enrolment_as_string + " for " + sid);
                    }
                }
            }

            // use the target if that is all that is available
            string target_size = GetElementAsString(r.Element("target_size"));
            if (enrolment == 0 && !string.IsNullOrEmpty(target_size)
                && !target_size.Contains("9999"))
            {
                if (Regex.Match(target_size, @"\d+").Success)
                {
                    string enrolment_as_string = Regex.Match(target_size, @"\d+").Value;
                    if (Int32.TryParse(enrolment_as_string, out int numeric_value))
                    {
                        if (numeric_value < 10000)
                        {
                            enrolment = numeric_value;
                        }
                    }
                    else
                    {
                        // what is going on?
                        _logger.Error("Odd enrolment string: " + enrolment_as_string + " for " + sid);
                    }
                }
            }
            s.study_enrolment = enrolment > 0 ? enrolment : null;

            string agemin = GetElementAsString(r.Element("agemin"));
            string agemin_units = GetElementAsString(r.Element("agemin_units"));
            string agemax = GetElementAsString(r.Element("agemax"));
            string agemax_units = GetElementAsString(r.Element("agemax_units"));

            if (Int32.TryParse(agemin, out int min))
            {
                s.min_age = min;
                if (agemin_units.StartsWith("Other"))
                {
                    // was not classified previously...
                    s.min_age_units = th.GetTimeUnits(agemin_units);
                }
                else
                {
                    s.min_age_units = agemin_units;
                }
                if (s.min_age_units != null)
                {
                    s.min_age_units_id = th.GetTimeUnitsId(s.min_age_units);
                }
            }


            if (Int32.TryParse(agemax, out int max))
            {
                if (max != 0)
                {
                    s.max_age = max;
                    if (agemax_units.StartsWith("Other"))
                    {
                        // was not classified previously...
                        s.max_age_units = th.GetTimeUnits(agemax_units);
                    }
                    else
                    {
                        s.max_age_units = agemax_units;
                    }
                    if (s.max_age_units != null)
                    {
                        s.max_age_units_id = th.GetTimeUnitsId(s.max_age_units);
                    }
                }
            }

            string gender = GetElementAsString(r.Element("gender"));
            if (gender.Contains("?? Unable to classify"))
            {
                gender = "Not provided";
            }

            s.study_gender_elig = gender;
            s.study_gender_elig_id = th.GetGenderEligId(gender);


            // Add study attribute records.

            // study contributors - Sponsor N.B. default below
            string sponsor_name = "No organisation name provided in source data";

            string primary_sponsor = GetElementAsString(r.Element("primary_sponsor"));
            if (!string.IsNullOrEmpty(primary_sponsor))
            {
                sponsor_name = sh.TidyOrgName(primary_sponsor, sid);
                string sponsor = sponsor_name.ToLower();
                if (sh.FilterOut_Null_OrgNames(sponsor) != "")
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
            string scientific_contact_givenname = GetElementAsString(r.Element("scientific_contact_givenname"));
            string scientific_contact_familyname = GetElementAsString(r.Element("scientific_contact_familyname"));
            string scientific_contact_affiliation = GetElementAsString(r.Element("scientific_contact_affiliation"));
            if (!string.IsNullOrEmpty(scientific_contact_givenname) || !string.IsNullOrEmpty(scientific_contact_familyname))
            {
                string givenname = scientific_contact_givenname ?? "";
                string familyname = scientific_contact_familyname ?? "";
                string full_name = (givenname + " " + familyname).Trim();
                study_lead = full_name;  // for later comparison
                string affiliation = scientific_contact_affiliation ?? "";
                study_contributors.Add(new StudyContributor(sid, 51, "Study Lead", null, null, full_name, affiliation));
            }

            // public contact
            string public_contact_givenname = GetElementAsString(r.Element("public_contact_givenname"));
            string public_contact_familyname = GetElementAsString(r.Element("public_contact_familyname"));
            string public_contact_affiliation = GetElementAsString(r.Element("public_contact_affiliation"));
            if (!string.IsNullOrEmpty(public_contact_givenname) || !string.IsNullOrEmpty(public_contact_familyname))
            {
                string givenname = public_contact_givenname ?? "";
                string familyname = public_contact_familyname ?? "";
                string full_name = (givenname + " " + familyname).Trim();
                if (full_name != study_lead)  // often duplicated
                {
                    string affiliation = public_contact_affiliation ?? "";
                    study_contributors.Add(new StudyContributor(sid, 56, "Public Contact", null, null, full_name, affiliation));
                }
            }

            // study features 
            XElement sf = r.Element("study_features");
            if (sf != null)
            {
                var study_feats = sf.Elements("StudyFeatures");
                if (study_feats != null && study_feats.Count() > 0)
                {
                    foreach (XElement f in study_feats)
                    {
                        int? ftype_id = GetElementAsInt(f.Element("ftype_id"));
                        string ftype = GetElementAsString(f.Element("ftype"));
                        int? fvalue_id = GetElementAsInt(f.Element("fvalue_id"));
                        string fvalue = GetElementAsString(f.Element("fvalue"));
                        study_features.Add(new DataHarvester.StudyFeature(sid, ftype_id, ftype, fvalue_id, fvalue));
                    }
                }
            }


            //study identifiers
            XElement sids = r.Element("secondary_ids");
            if (sids != null)
            {
                var secondary_ids = sids.Elements("Secondary_Id");
                if (secondary_ids != null && secondary_ids.Count() > 0)
                {
                    foreach (XElement id in secondary_ids)
                    {
                        int? sec_id_source = GetElementAsInt(id.Element("sec_id_source"));
                        string processed_id = GetElementAsString(id.Element("processed_id"));
                        if (sec_id_source == null)
                        {
                            study_identifiers.Add(new StudyIdentifier(sid, processed_id, 14, "Sponsor ID", null, sponsor_name));
                        }
                        else
                        {
                            if (sec_id_source == 102000)
                            {
                                study_identifiers.Add(new StudyIdentifier(sid, processed_id, 41, "Regulatory Body ID", 102000, "Anvisa (Brazil)"));
                            }
                            else if (sec_id_source == 102001)
                            {
                                study_identifiers.Add(new StudyIdentifier(sid, processed_id, 12, "Ethics Review ID", 102001, "Comitê de Ética em Pesquisa (local) (Brazil)"));
                            }
                            else
                            {
                                study_identifiers.Add(new StudyIdentifier(sid, processed_id, 11, "Trial Registry ID", sec_id_source, ih.get_source_name(sec_id_source)));
                            }
                        }
                    }
                }
            }

            // study conditions
            XElement cl = r.Element("condiiton_list");
            if (cl != null)
            {
                var conditions = cl.Elements("StudyCondition");
                if (conditions != null && conditions.Count() > 0)
                {
                    foreach (XElement cn in conditions)
                    {
                        string condition = GetElementAsString(cn.Element("condition"));
                        if (!string.IsNullOrEmpty(condition))
                        {
                            char[] chars_to_trim = { ' ', '?', ':', '*', '/', '-', '_', '+', '=' };
                            string cond = condition.Trim(chars_to_trim);
                            if (!string.IsNullOrEmpty(cond) && cond.ToLower() != "not applicable" && cond.ToLower() != "&quot")
                            {
                                string code = GetElementAsString(cn.Element("code"));
                                string code_system = GetElementAsString(cn.Element("code_system"));

                                if (code == null)
                                {
                                    study_topics.Add(new StudyTopic(sid, 13, "Condition", cond));
                                }
                                else
                                {
                                    if (code_system == "ICD 10")
                                    {
                                        study_topics.Add(new StudyTopic(sid, 13, "Condition", cond, 12, code, ""));
                                    }
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
            string sd_oid = hh.CreateMD5(sid + object_display_title);

            int? pub_year = registration_date?.year;

            string source_name = ih.get_source_name(source_id);
            data_objects.Add(new DataObject(sd_oid, sid, object_display_title, pub_year, 23, "Text", 13, "Trial Registry entry",
                source_id, source_name, 12, download_datetime));

            data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
                                "Study short name :: object type", true));

            string remote_url = GetElementAsString(r.Element("remote_url"));
            data_object_instances.Add(new ObjectInstance(sd_oid, source_id, source_name,
                                remote_url, true, 35, "Web text"));

            if (registration_date != null)
            {
                data_object_dates.Add(new ObjectDate(sd_oid, 15, "Created", registration_date.year,
                          registration_date.month, registration_date.day, registration_date.date_string));
            }

            string rec_date = GetElementAsString(r.Element("record_date"));
            if (rec_date != null)
            {
                SplitDate record_date = dh.GetDatePartsFromISOString(rec_date);
                data_object_dates.Add(new ObjectDate(sd_oid, 18, "Updated", record_date.year,
                          record_date.month, record_date.day, record_date.date_string));

            }

            string results_url_link = GetElementAsString(r.Element("results_url_link"));
            string results_date_posted = GetElementAsString(r.Element("results_date_posted"));
            // there may be (rarely) a results link... (exclude those on CTG - should be picked up there)
            if (!string.IsNullOrEmpty(results_url_link))
            {
                if (results_url_link.Contains("http") && !results_url_link.ToLower().Contains("clinicaltrials.gov"))
                {
                    object_display_title = name_base + " :: " + "Results summary";
                    sd_oid = hh.CreateMD5(sid + object_display_title);
                    SplitDate results_date = null;
                    if (results_date_posted != null)
                    {
                        results_date = dh.GetDatePartsFromISOString(results_date_posted);
                    }

                    int? posted_year = results_date?.year;

                    // (in practice may not be in the registry)
                    data_objects.Add(new DataObject(sd_oid, sid, object_display_title, posted_year, 
                                     23, "Text", 28, "Trial registry results summary",
                                     source_id, source_name, 12, download_datetime));

                    data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
                                        "Study short name :: object type", true));

                    string url_link = Regex.Match(results_url_link, @"(http|https)://[\w-]+(\.[\w-]+)+([\w\.,@\?\^=%&:/~\+#-]*[\w@\?\^=%&/~\+#-])?").Value;
                    data_object_instances.Add(new ObjectInstance(sd_oid, source_id, source_name,
                                        url_link, true, 35, "Web text"));

                    if (results_date != null)
                    {
                        data_object_dates.Add(new ObjectDate(sd_oid, 15, "Created", results_date.year,
                                  results_date.month, results_date.day, results_date.date_string));
                    }
                }
            }


            // there may be (rarely) a protocol link...(exclude those simply referring to CTG - should be picked up there)
            string results_url_protocol = GetElementAsString(r.Element("results_url_protocol"));
            if (!string.IsNullOrEmpty(results_url_protocol))
            {
                string prot_url = results_url_protocol.ToLower();
                if (prot_url.Contains("http") && !prot_url.Contains("clinicaltrials.gov"))
                {
                    // presumed to be a download or a web reference
                    string resource_type = "";
                    int resource_type_id = 0;
                    string url_link = "";
                    int url_start = prot_url.IndexOf("http");
                    if (results_url_protocol.Contains(".pdf"))
                    {
                        resource_type = "PDF";
                        resource_type_id = 11;
                        int pdf_end = prot_url.IndexOf(".pdf");
                        url_link = results_url_protocol.Substring(url_start, pdf_end - url_start + 4);

                    }
                    else if (prot_url.Contains(".doc"))
                    {
                        resource_type = "Word doc";
                        resource_type_id = 16;
                        if (prot_url.Contains(".docx"))
                        {
                            int docx_end = prot_url.IndexOf(".docx");
                            url_link = results_url_protocol.Substring(url_start, docx_end - url_start + 5);
                        }
                        else
                        {
                            int doc_end = prot_url.IndexOf(".doc");
                            url_link = results_url_protocol.Substring(url_start, doc_end - url_start + 4);
                        }
                    }
                    else
                    {
                        // most probably some sort of web reference
                        resource_type = "Web text";
                        resource_type_id = 35;
                        url_link = Regex.Match(results_url_protocol, @"(http|https)://[\w-]+(\.[\w-]+)+([\w\.,@\?\^=%&:/~\+#-]*[\w@\?\^=%&/~\+#-])?").Value;
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
                    sd_oid = hh.CreateMD5(sid + object_display_title);

                    // almost certainly not in the registry
                    data_objects.Add(new DataObject(sd_oid, sid, object_display_title, pub_year, 23, "Text", object_type_id, object_type,
                    null, null, 11, download_datetime));

                    data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
                                        "Study short name :: object type", true));

                    data_object_instances.Add(new ObjectInstance(sd_oid, source_id, source_name,
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
                        if (ih.CheckIfIndividual(orgname))
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


        public void StoreData(Study s, string db_conn)
        {
            // store study
            StudyInDB st = new StudyInDB(s);
            _storage_repo.StoreStudy(st, db_conn);

            StudyCopyHelpers sch = new StudyCopyHelpers();
            ObjectCopyHelpers och = new ObjectCopyHelpers();

            // store study attributes
            if (s.identifiers.Count > 0)
            {
                _storage_repo.StoreStudyIdentifiers(sch.study_ids_helper, s.identifiers, db_conn);
            }

            if (s.titles.Count > 0)
            {
                _storage_repo.StoreStudyTitles(sch.study_titles_helper, s.titles, db_conn);
            }

            if (s.features.Count > 0)
            {
                _storage_repo.StoreStudyFeatures(sch.study_features_helper, s.features, db_conn);
            }

            if (s.topics.Count > 0)
            {
                _storage_repo.StoreStudyTopics(sch.study_topics_helper, s.topics, db_conn);
            }

            if (s.contributors.Count > 0)
            {
                _storage_repo.StoreStudyContributors(sch.study_contributors_helper, s.contributors, db_conn);
            }

            // store data objects and dataset properties
            if (s.data_objects.Count > 0)
            {
                _storage_repo.StoreDataObjects(och.data_objects_helper, s.data_objects, db_conn);
            }

            // store data object attributes
            if (s.object_dates.Count > 0)
            {
                _storage_repo.StoreObjectDates(och.object_dates_helper, s.object_dates, db_conn);
            }

            if (s.object_instances.Count > 0)
            {
                _storage_repo.StoreObjectInstances(och.object_instances_helper, s.object_instances, db_conn);
            }

            if (s.object_titles.Count > 0)
            {
                _storage_repo.StoreObjectTitles(och.object_titles_helper, s.object_titles, db_conn);
            }
        }


        private string GetElementAsString(XElement e) => (e == null) ? null : (string)e;

        private string GetAttributeAsString(XAttribute a) => (a == null) ? null : (string)a;


        private int? GetElementAsInt(XElement e)
        {
            string evalue = GetElementAsString(e);
            if (string.IsNullOrEmpty(evalue))
            {
                return null;
            }
            else
            {
                if (Int32.TryParse(evalue, out int res))
                    return res;
                else
                    return null;
            }
        }

        private int? GetAttributeAsInt(XAttribute a)
        {
            string avalue = GetAttributeAsString(a);
            if (string.IsNullOrEmpty(avalue))
            {
                return null;
            }
            else
            {
                if (Int32.TryParse(avalue, out int res))
                    return res;
                else
                    return null;
            }
        }


        private bool GetElementAsBool(XElement e)
        {
            string evalue = GetElementAsString(e);
            if (evalue != null)
            {
                return (evalue.ToLower() == "true" || evalue.ToLower()[0] == 'y') ? true : false;
            }
            else
            {
                return false;
            }
        }

        private bool GetAttributeAsBool(XAttribute a)
        {
            string avalue = GetAttributeAsString(a);
            if (avalue != null)
            {
                return (avalue.ToLower() == "true" || avalue.ToLower()[0] == 'y') ? true : false;
            }
            else
            {
                return false;
            }
        }

    }

}

