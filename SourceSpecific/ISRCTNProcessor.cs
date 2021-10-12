using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using Serilog;

namespace DataHarvester.isrctn
{
    public class ISRCTNProcessor : IStudyProcessor
    {
        IMonitorDataLayer _mon_repo;
        ILogger _logger;

        public ISRCTNProcessor(IMonitorDataLayer mon_repo, ILogger logger)
        {
            _mon_repo = mon_repo;
            _logger = logger;
        }

        public Study ProcessData(XmlDocument d, DateTime? download_datetime)
        {
            Study s = new Study();

            List<StudyIdentifier> identifiers = new List<StudyIdentifier>();
            List<StudyTitle> titles = new List<StudyTitle>();
            List<StudyContributor> contributors = new List<StudyContributor>();
            List<StudyReference> references = new List<StudyReference>();
            List<StudyTopic> topics = new List<StudyTopic>();
            List<StudyFeature> features = new List<StudyFeature>();

            List<DataObject> data_objects = new List<DataObject>();
            List<ObjectTitle> object_titles = new List<ObjectTitle>();
            List<ObjectDate> object_dates = new List<ObjectDate>();
            List<ObjectInstance> object_instances = new List<ObjectInstance>();

            MD5Helpers hh = new MD5Helpers();
            StringHelpers sh = new StringHelpers(_logger);
            DateHelpers dh = new DateHelpers();
            TypeHelpers th = new TypeHelpers();
            IdentifierHelpers ih = new IdentifierHelpers();

            SplitDate date_assigned = null;
            SplitDate last_edit = null;
            string study_description = null;
            string sharing_statement = null;

            // First convert the XML document to a Linq XML Document.

            XDocument xDoc = XDocument.Load(new XmlNodeReader(d));

            // Obtain the main top level elements of the registry entry.

            XElement r = xDoc.Root;

            string sid = GetElementAsString(r.Element("isctrn_id"));
            s.sd_sid = sid;
            s.datetime_of_data_fetch = download_datetime;

            // get basic study attributes
            string study_name = GetElementAsString(r.Element("study_name"));
            study_name = sh.ReplaceApos(study_name);
            s.display_title = study_name;   // = public title, default
            s.datetime_of_data_fetch = download_datetime;

            titles.Add(new StudyTitle(sid, s.display_title, 15, "Registry public title", true, "From ISRCTN"));

            // study status from trial_status and recruitment_status
            // record for now and see what is available
            string trial_status = GetElementAsString(r.Element("trial_status"));
            string recruitment_status = GetElementAsString(r.Element("recruitment_status"));
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

            // study registry entry dates
            string d_assigned = GetElementAsString(r.Element("date_assigned"));
            if (d_assigned != null)
            {
                date_assigned = dh.GetDatePartsFromISOString(d_assigned.Substring(0, 10));
            }

            string d_edited = GetElementAsString(r.Element("last_edited"));
            if (d_edited != null)
            {
                last_edit = dh.GetDatePartsFromISOString(d_edited.Substring(0, 10));
            }


            // study sponsors
            var sponsor = r.Element("sponsor");
            if (sponsor != null)
            {
                var items = sponsor.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        string item_value = GetElementAsString(item.Element("item_value"));
                        if (item_name == "Organisation")
                        {
                            if (sh.AppearsGenuineOrgName(item_value))
                            {
                                contributors.Add(new StudyContributor(sid, 54, "Trial Sponsor",
                                    null, sh.TidyOrgName(item_value, sid)));
                            }
                        }
                       
                    }
                }
            }

            string study_sponsor = "";
            if (contributors.Count > 0)
            {
                study_sponsor = contributors[0].organisation_name;
            }


            var contacts = r.Element("contacts");
            if (contacts != null)
            {
                var items = contacts.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    StudyContributor c = null; 				
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        string item_value = GetElementAsString(item.Element("item_value"));

                        switch (item_name)
                        {
                            case "Type":
                                {
                                    // starts a new contact record...
                                    // also need to store any pre-existing record
                                    if (c != null)
                                    {
                                        if (sh.CheckPersonName(c.person_full_name))
                                        {
                                            c.person_full_name = sh.TidyPersonName(c.person_full_name);
                                            if (c.person_full_name != "")
                                            {
                                                contributors.Add(c);
                                            }
                                        }
                                    }

                                    c = new StudyContributor(sid, null, null, null, null, null, null);
                                    if (item_value == "Scientific")
                                    {
                                        c.contrib_type_id = 51;
                                        c.contrib_type = "Study Lead";
                                        c.is_individual = true;
                                    }
                                    else if (item_value == "Public")
                                    {
                                        c.contrib_type_id = 56;
                                        c.contrib_type = "Public contact";
                                        c.is_individual = true;
                                    }
                                    else
                                    {
                                        c.contrib_type_id = 0;
                                        c.contrib_type = item_value;
                                        c.is_individual = true;
                                    }
                                    break;
                                }
                            case "Primary contact":
                                {
                                    c.person_full_name = sh.ReplaceApos(item_value);
                                    break;
                                }
                            case "Additional contact":
                                {
                                    c.person_full_name = sh.ReplaceApos(item_value);
                                    break;
                                }
                            case "ORCID ID":
                                {
                                    if (item_value.Contains("/"))
                                    {
                                        c.orcid_id = item_value.Substring(item_value.LastIndexOf("/") + 1);
                                    }
                                    else
                                    {
                                        c.orcid_id = item_value;
                                    }
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
                        if (sh.CheckPersonName(c.person_full_name))
                        {
                            c.person_full_name = sh.TidyPersonName(c.person_full_name);
                            if (c.person_full_name != "")
                            {
                                contributors.Add(c);
                            }
                        }
                    }
                }
            }


            var funders = r.Element("funders");
            if (funders != null)
            {
                var items = funders.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        if (item_name == "Funder name")
                        {
                            string item_value = GetElementAsString(item.Element("item_value"));
                            if (sh.AppearsGenuineOrgName(item_value))
                            {
                                // check a funder is not simply the sponsor...
                                string funder = sh.TidyOrgName(item_value, sid);
                                if (funder != study_sponsor)
                                {
                                    contributors.Add(new StudyContributor(sid, 58, "Study Funder", null, funder));
                                }

                            }
                        }
                    }
                }
            }

            // study identifiers
            // do the isrctn id first...
            identifiers.Add(new StudyIdentifier(sid, sid, 11, "Trial Registry ID", 100126, "ISRCTN", date_assigned.date_string, null));

            // then any others that might be listed
            var idents = r.Element("identifiers");
            if (idents != null)
            {
                var items = idents.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        string item_value = GetElementAsString(item.Element("item_value"));

                        switch (item_name)
                        {

                            case "Protocol/serial number":
                                {
                                    IdentifierDetails idd;
                                    if (item_value.Contains(";"))
                                    {
                                        string[] iditems = item_value.Split(";");
                                        foreach (string iditem in iditems)
                                        {
                                            string item2 = iditem.Trim();
                                            idd = ih.GetISRCTNIdentifierProps(item2, study_sponsor);
                                            if (idd.id_type != "Protocol version")
                                            {
                                                identifiers.Add(new StudyIdentifier(sid, idd.id_value, idd.id_type_id, idd.id_type,
                                                                                       idd.id_org_id, idd.id_org, null, null));
                                            }
                                        }
                                    }
                                    else if (item_value.Contains(",") &&
                                        (item_value.ToLower().Contains("iras") || item_value.ToLower().Contains("hta")))
                                    {
                                        string[] iditems = item_value.Split(",");
                                        foreach (string iditem in iditems)
                                        {
                                            string item2 = iditem.Trim();
                                            idd = ih.GetISRCTNIdentifierProps(item2, study_sponsor);
                                            if (idd.id_type != "Protocol version")
                                            {
                                                identifiers.Add(new StudyIdentifier(sid, idd.id_value, idd.id_type_id, idd.id_type,
                                                                                       idd.id_org_id, idd.id_org, null, null));
                                            }
                                        }
                                    }
                                    else
                                    {
                                        idd = ih.GetISRCTNIdentifierProps(item_value, study_sponsor);
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
                                    identifiers.Add(new StudyIdentifier(sid, item_value, 11, "Trial Registry ID", 100120, "Clinicaltrials.gov", null, null));
                                    break;
                                }
                            case "EudraCT number":
                                {
                                    identifiers.Add(new StudyIdentifier(sid, item_value, 11, "Trial Registry ID", 100123, "EU Clinical Trials Register", null, null));
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
            }


            // design info

            string listed_condition = "";   //  defined here to use in later comparison

            string PIS_details = "";
            var study_info = r.Element("study_info");
            if (study_info != null)
            {
                var items = study_info.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        string item_value = GetElementAsString(item.Element("item_value"));

                        switch (item_name)
                        {
                            case "Scientific title":
                                {
                                    string study_title = sh.ReplaceApos(item_value).Trim();
                                    if (study_title.ToLower() != study_name.ToLower())
                                    {
                                        titles.Add(new StudyTitle(sid, sh.ReplaceApos(item_value), 16, "Registry scientific title", false, "From ISRCTN"));
                                    }
                                    break;
                                }
                            case "Acronym":
                                {
                                    titles.Add(new StudyTitle(sid, item_value, 14, "Acronym or Abbreviation", false, "From ISRCTN"));
                                    break;
                                }
                            case "Study hypothesis":
                                {
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        item_value = sh.StringClean(item_value);
                                        if (!item_value.ToLower().StartsWith("study"))
                                        {
                                            item_value = "Study hypothesis: " + item_value;
                                        }
                                        study_description = item_value;
                                    }
                                    break;
                                }
                            case "Primary study design":
                                {
                                    if (item_value == "Interventional")
                                    {
                                        s.study_type = "Interventional";
                                        s.study_type_id = 11;
                                    }
                                    else if (item_value == "Observational")
                                    {
                                        s.study_type = "Observational";
                                        s.study_type_id = 12;
                                    }
                                    else if (item_value == "Other")
                                    {
                                        s.study_type = "Other";
                                        s.study_type_id = 16;
                                    }
                                    break;
                                }
                            case "Secondary study design":
                                {
                                    string design = item_value.ToLower().Replace("randomized", "randomised");
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
                                    switch (item_value)
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
                                    string design = item_value.ToLower().Replace("open label", "open-label").Replace("single blind", "single-blind");
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
                                    if (!item_value.StartsWith("Not available") && !item_value.StartsWith("Not applicable"))
                                    {
                                        if (item_value.Contains("<a href"))
                                        {
                                            // try and create a data object later corresponding to the PIS (object and instance only)
                                            PIS_details = item_value;
                                        }
                                    }
                                    break;
                                }
                            case "Condition":
                                {
                                    listed_condition = item_value;
                                    break;
                                }
                            case "Drug names":
                                {
                                    topics.Add(new StudyTopic(sid, 12, "chemical / agent", item_value));
                                    break;
                                }
                            case "Phase":
                                {
                                    int value_id = 0;
                                    string value_name = "";
                                    switch (item_value)
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
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        item_value = sh.StringClean(item_value);
                                        if (!string.IsNullOrEmpty(study_description))
                                        {
                                            study_description += "\n";
                                        }
                                        if (item_value.ToLower().StartsWith("primary"))
                                        {
                                            study_description += item_value;
                                        }
                                        else
                                        {
                                            study_description += "Primary outcome(s): " + item_value;
                                        }
                                    }
                                    break;
                                }
                            case "Overall trial start date":
                                {
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        CultureInfo eu_cultureinfo = new CultureInfo("fr-FR");
                                        if (DateTime.TryParse(item_value, eu_cultureinfo, DateTimeStyles.None, out DateTime start_date))
                                        {
                                            s.study_start_year = start_date.Year;
                                            s.study_start_month = start_date.Month;
                                        }
                                    }
                                    break;
                                }
                            case "Reason abandoned (if study stopped)":
                                {
                                    item_value = sh.StringClean(item_value);
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        if (!string.IsNullOrEmpty(study_description))
                                        {
                                            study_description += "\n";
                                        }
                                        study_description += "Reason study stopped: " + sh.StringClean(item_value);
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
            }


            if (listed_condition != "")
            {
                topics.Add(new StudyTopic(sid, 13, "condition", listed_condition));
            }
            else
            {
                // these tend to be very general - high level classifcvations
                string conditions = GetElementAsString(r.Element("condition_category"));
                if (conditions.Contains(","))
                {
                    // add topics
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
            }


            // eligibility 
            var eligibility = r.Element("eligibility");
            if (eligibility != null)
            {
                var items = eligibility.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        string item_value = GetElementAsString(item.Element("item_value"));

                        switch (item_name)
                        {
                            case "Age group":
                                {
                                    switch (item_value)
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
                                    switch (item_value)
                                    {
                                        case "Both":
                                            {
                                                s.study_gender_elig_id = 900;
                                                s.study_gender_elig = "All";
                                                break;
                                            }
                                        case "Female":
                                            {
                                                s.study_gender_elig_id = 905;
                                                s.study_gender_elig = "Female";
                                                break;
                                            }
                                        case "Male":
                                            {
                                                s.study_gender_elig_id = 910;
                                                s.study_gender_elig = "Male";
                                                break;
                                            }
                                        case "Not Specified":
                                            {
                                                s.study_gender_elig_id = 915;
                                                s.study_gender_elig = "Not provided";
                                                break;
                                            }
                                    }
                                    break;
                                }
                            case "Target number of participants":
                                {
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        s.study_enrolment = item_value;
                                    }
                                    break;
                                }
                            case "Total final enrolment":
                                {
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        // if available replace with this...
                                        s.study_enrolment = item_value;
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
            }


            // DATA OBJECTS and their attributes
            // initial data object is the ISRCTN registry entry

            int pub_year = 0;
            if (date_assigned != null)
            {
                pub_year = (int)date_assigned.year;
            }
            string object_display_title = s.display_title + " :: ISRCTN registry entry";

            // create hash Id for the data object
            string sd_oid = hh.CreateMD5(sid + object_display_title);

            DataObject dobj = new DataObject(sd_oid, sid, object_display_title, pub_year,
                  23, "Text", 13, "Trial Registry entry", 100126, "ISRCTN", 12, download_datetime);

            dobj.doi = GetElementAsString(r.Element("doi"));
            dobj.doi_status_id = 1;
            data_objects.Add(dobj);

            // data object title is the single display title...
            object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 
                                                     22, "Study short name :: object type", true));
            if (last_edit != null)
            {
                object_dates.Add(new ObjectDate(sd_oid, 18, "Updated", 
                                  last_edit.year, last_edit.month, last_edit.day, last_edit.date_string));
            }

            if (date_assigned != null)
            {
                object_dates.Add(new ObjectDate(sd_oid, 15, "Created",
                                  date_assigned.year, date_assigned.month, date_assigned.day, date_assigned.date_string));
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
                    sd_oid = hh.CreateMD5(sid + object_display_title);

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
            var publications = r.Element("publications");
            if (publications != null)
            {
                var items = publications.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        string item_value = GetElementAsString(item.Element("item_value"));

                        switch (item_name)
                        {
                            case "Publication and dissemination plan":
                                {
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        if (item_value.Contains("IPD sharing statement:<br>"))
                                        {
                                            item_value = item_value.Substring(item_value.IndexOf("IPD sharing statement") + 26);
                                            sharing_statement = "IPD sharing statement: " + sh.StringClean(item_value);
                                        }

                                        else if (item_value.Contains("IPD sharing statement"))
                                        {
                                            item_value = item_value.Substring(item_value.IndexOf("IPD sharing statement") + 21);
                                            sharing_statement =  "IPD sharing statement: " + sh.StringClean(item_value);
                                        }
                                        else
                                        {
                                            sharing_statement = sh.StringClean("General dissemination plan: " + item_value);
                                        }
                                    }
                                    break;
                                }
                            case "Participant level data":
                                {
                                    if (item_value != "Not provided at time of registration")
                                    {
                                        if (!string.IsNullOrEmpty(sharing_statement))
                                        {
                                            sharing_statement += "\n";
                                        }

                                        sharing_statement += sh.StringClean("IPD: " + item_value);
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
                                    string itemline = item_value.Replace("<br>", "").Replace("<br/>", "");
                                    string refs = itemline.Replace("<a", "||").Replace("</a>", "||").Replace(">", "||");
                                    string[] ref_items = refs.Split("||");
                                    for (int j = 0; j < ref_items.Length; j += 3)
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
                                                    references.Add(new StudyReference(sid, pmid.ToString(), ref_items[j + 2], ref_items[j], null));

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

                                    string refs = item_value.Replace("<a", "||").Replace("</a>", "||").Replace(">", "||");
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
                                                sd_oid = hh.CreateMD5(sid + object_display_title);

                                                data_objects.Add(new DataObject(sd_oid, sid, s.display_title + " :: " + object_type, s.study_start_year,
                                                            23, "Text", object_type_id, object_type,
                                                            101418, "Servier", 11, download_datetime));
                                                object_titles.Add(new ObjectTitle(sd_oid, s.display_title + " :: " + object_type,
                                                            22, "Study short name :: object type", true));
                                                object_instances.Add(new ObjectInstance(sd_oid, 101418, "Servier",
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
            }

            // possible additional files
            var additional_files = r.Element("additional_files");
            if (additional_files != null)
            {
                var items = additional_files.Elements("Item");
                if (items != null && items.Count() > 0)
                {
                    foreach (XElement item in items)
                    {
                        string item_name = GetElementAsString(item.Element("item_name"));
                        string item_value = GetElementAsString(item.Element("item_value"));

                        // need to correct an extraction error here...
                        string link = item_value.Replace("//editorial", "/editorial");

                        // create object details
                        string object_type, test_name;
                        int object_type_id;
                        test_name = item_name.ToLower();

                        // N.B. a few documents categorised manually as their name includes no 
                        // clue to their content...
                        if (test_name.Contains("results"))
                        {
                            object_type = "Unpublished Study Report";
                            object_type_id = 85;
                        }
                        else if (test_name.Contains("protocol")
                                          || item_name == "ISRCTN23416732_v5_13June2018.pdf"
                                          || item_name == "ISRCTN36746902 _V2.2_final_30Jan20.pdf"
                                          || item_name == "ISRCTN84288963_v8.0_21062018.docx.pdf")
                        {
                            object_type = "Study Protocol";
                            object_type_id = 11;
                        }
                        else if (test_name.Contains("pis") || test_name.Contains("participant")
                                   || item_name == "ISRCTN88166769.pdf")
                        {
                            object_type = "Patient information sheets";
                            object_type_id = 19;
                        }
                        else if (test_name.Contains("sap") || test_name.Contains("statistical")
                                    || item_name == "ISRCTN14148239_V1.0_21Oct19.pdf")
                        {
                            object_type = "Statistical analysis plan";
                            object_type_id = 22;
                        }
                        else if (test_name.Contains("consent"))
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
                        if (item_name.ToLower().EndsWith("pdf"))
                        {
                            res_type_id = 11;
                            res_type = "PDF";
                        }
                        else if (item_name.ToLower().EndsWith("docx") || item_name.ToLower().EndsWith("doc"))
                        {
                            res_type_id = 16;
                            res_type = "Word doc";
                        }
                        else if (item_name.ToLower().EndsWith("pptx") || item_name.ToLower().EndsWith("ppt"))
                        {
                            res_type_id = 20;
                            res_type = "PowerPoint";
                        }

                        object_display_title = s.display_title + " :: " + item_name;
                        sd_oid = hh.CreateMD5(sid + object_display_title);

                        data_objects.Add(new DataObject(sd_oid, sid, object_display_title, null,
                                    23, "Text", object_type_id, object_type, 100126, "ISRCTN", 11, download_datetime));
                        object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
                                    20, "Unique data object title", true));
                        object_instances.Add(new ObjectInstance(sd_oid, 100126, "ISRCTN",
                                link, true, res_type_id, res_type));
                        break;
                    }

                }
            }

             
            // possible object of a trial web site if one exists for this study
            string trial_website = GetElementAsString(r.Element("trial_website"));
            if (!string.IsNullOrEmpty(trial_website))
            {
                // first check website link does not provide a 404
                if (true) //await HtmlHelpers.CheckURLAsync(fs.trial_website))
                {
                    object_display_title = s.display_title + " :: website";
                    sd_oid = hh.CreateMD5(sid + object_display_title);

                    data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
                            23, "Text", 134, "Website", null, study_sponsor, 12, download_datetime));
                    object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 
                                                         22, "Study short name :: object type", true));
                    ObjectInstance instance = new ObjectInstance(sd_oid, null, study_sponsor,
                            trial_website, true, 35, "Web text");
                    instance.url_last_checked = DateTime.Today;
                    object_instances.Add(instance);
                } 
            }


            // edit contributors - try to ensure properly categorised

            if (contributors.Count > 0)
            {
                foreach (StudyContributor sc in contributors)
                {
                    if (!sc.is_individual)
                    {
                        // identify individuals down as organisations

                        string orgname = sc.organisation_name.ToLower();
                        if (ih.CheckIfIndividual(orgname))
                        {
                            sc.person_full_name = sh.TidyPersonName(sc.organisation_name);
                            sc.organisation_name = null;
                            sc.is_individual = true;
                        }
                    }
                    else
                    {
                        // check if a group inserted as an individual

                        string fullname = sc.person_full_name.ToLower();
                        if (ih.CheckIfOrganisation(fullname))
                        {
                            sc.organisation_name = sh.TidyOrgName(sid, sc.person_full_name);
                            sc.person_full_name = null;
                            sc.is_individual = false;
                        }
                    }
                }
            }



            s.brief_description = study_description;
            s.data_sharing_statement = sharing_statement;

            s.identifiers = identifiers;
            s.titles = titles;
            s.contributors = contributors;
            s.references = references;
            s.topics = topics;
            s.features = features;

            s.data_objects = data_objects;
            s.object_titles = object_titles;
            s.object_dates = object_dates;
            s.object_instances = object_instances;

            return s;

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
