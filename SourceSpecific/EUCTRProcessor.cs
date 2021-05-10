using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using Serilog;

namespace DataHarvester.euctr
{
    public class EUCTRProcessor : IStudyProcessor
    {
        IStorageDataLayer _storage_repo;
        IMonitorDataLayer _mon_repo;
        ILogger _logger;

        public EUCTRProcessor(IStorageDataLayer storage_repo, IMonitorDataLayer mon_repo, ILogger logger)
        {
            _storage_repo = storage_repo;
            _mon_repo = mon_repo;
            _logger = logger;
        }

        public Study ProcessData(XmlDocument d, DateTime? download_datetime)
        {
            Study s = new Study();
            List<StudyIdentifier> identifiers = new List<StudyIdentifier>();
            List<StudyTitle> titles = new List<StudyTitle>();
            List<StudyContributor> contributors = new List<StudyContributor>();
            List<StudyTopic> topics = new List<StudyTopic>();
            List<StudyFeature> features = new List<StudyFeature>();

            List<DataObject> data_objects = new List<DataObject>();
            List<ObjectTitle> object_titles = new List<ObjectTitle>();
            List<ObjectInstance> object_instances = new List<ObjectInstance>();

            MD5Helpers hh = new MD5Helpers();
            StringHelpers sh = new StringHelpers(_logger, _mon_repo);
            HtmlHelpers mh = new HtmlHelpers(_logger);

            // First convert the XML document to a Linq XML Document.

            XDocument xDoc = XDocument.Load(new XmlNodeReader(d));

            // Obtain the main top level elements of the registry entry.

            XElement r = xDoc.Root;

            string sid = GetElementAsString(r.Element("eudract_id"));
            s.sd_sid = sid;
            s.datetime_of_data_fetch = download_datetime;


            // By defintion with the EU CTR
            s.study_type = "Interventional";
            s.study_type_id = 11;

            s.study_status = GetElementAsString(r.Element("trial_status")); 
            switch (s.study_status)
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
                        s.study_status_id = 0;
                        break;
                    }
            }


            // study start year and month
            // public string start_date { get; set; }  in yyyy-MM-dddd format
            string start_date = GetElementAsString(r.Element("start_date"));
            if (DateTime.TryParseExact(start_date, "yyyy-MM-dd", new CultureInfo("en-UK"), DateTimeStyles.AssumeLocal, out DateTime start))
            {
                s.study_start_year = start.Year;
                s.study_start_month = start.Month;
            }

            // contributor - sponsor
            string sponsor_name = "No organisation name provided in source data";
            string sponsor = GetElementAsString(r.Element("sponsor_name"));
            if (sh.FilterOut_Null_OrgNames(sponsor?.ToLower()) != "")
            {
                sponsor_name = sh.TidyOrgName(sponsor, sid);
                string lower_sponsor = sponsor_name.ToLower();
                if (!string.IsNullOrEmpty(lower_sponsor) && lower_sponsor.Length > 1
                    && lower_sponsor != "dr" && lower_sponsor != "no profit")
                {
                    contributors.Add(new StudyContributor(sid, 54, "Trial Sponsor", null, sponsor_name, null, null));
                }
            }

            // may get funders or other supporting orgs
            var sponsors = r.Element("sponsors");
            if (sponsors != null)
            { 
                var detail_lines = sponsors.Elements("DetailLine");
                if (detail_lines != null && detail_lines.Count() > 0)
                {
                    foreach (XElement dline in detail_lines)
                    {
                        string item_name = GetElementAsString(dline.Element("item_name"));
                        if (item_name == "Name of organisation providing support")
                        {
                            var values = dline.Elements("values");
                            if (values != null && values.Count() > 0)
                            {
                                string org_value = GetElementAsString(values.First());
                                // check a funder is not simply the sponsor...
                                if (sh.FilterOut_Null_OrgNames(org_value?.ToLower()) != "")
                                {
                                    string funder = sh.TidyOrgName(org_value, sid);
                                    if (funder != sponsor_name)
                                    {
                                        string fund = funder.ToLower();
                                        if (!string.IsNullOrEmpty(fund) && fund.Length > 1
                                        && fund != "dr" && fund != "no profit")
                                        {
                                            contributors.Add(new StudyContributor(sid, 58, "Study Funder", null, funder, null, null));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }


            // study identifiers
            // do the eu ctr id first...
            identifiers.Add(new StudyIdentifier(sid, sid, 11, "Trial Registry ID", 100123, "EU Clinical Trials Register", null, null));

            // do the sponsor's id
            string sponsor_id = GetElementAsString(r.Element("sponsor_id"));
            if (!string.IsNullOrEmpty(sponsor_id))
            {
                if (!string.IsNullOrEmpty(sponsor_name))
                {
                    identifiers.Add(new StudyIdentifier(sid, sponsor_id, 14, "Sponsor ID", null, sponsor_name, null, null));
                }
                else
                {
                    identifiers.Add(new StudyIdentifier(sid, sponsor_id, 14, "Sponsor ID", 12, "No organisation name provided in source data" , null, null));
                }
            }


            // identifier section actually seems to have titles
            var idents = r.Element("identifiers");
            if (idents != null)
            {
                var detail_lines = idents.Elements("DetailLine");
                if (detail_lines != null && detail_lines.Count() > 0)
                {
                    foreach (XElement dline in detail_lines)
                    {
                        string item_code = GetElementAsString(dline.Element("item_code"));
                        switch (item_code)
                        {
                            case "A.3":
                                {
                                    // may be multiple
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        foreach (XElement value in values)
                                        {
                                            string name = GetElementAsString(value);
                                            if (name != null && name.Length >= 4)
                                            {
                                                string st_name = name.Trim().ToLower();
                                                if (st_name != "n.a." && st_name != "none" &&
                                                    !st_name.StartsWith("see ") && !st_name.StartsWith("not avail") &&
                                                    !st_name.StartsWith("not applic") && !st_name.StartsWith("non applic") && !st_name.StartsWith("non aplic") &&
                                                    !st_name.StartsWith("no applic") && !st_name.StartsWith("no aplic") && !st_name.StartsWith("not aplic") &&
                                                    st_name != "not done" && st_name != "same as above" && st_name != "in preparation" &&
                                                    !st_name.StartsWith("non dispo") && st_name != "non fornito")
                                                {
                                                    titles.Add(new StudyTitle(sid, name, 16, "Trial Registry title", false));
                                                }
                                            }
                                        }
                                    }
                                    break;
                                }
                            case "A.3.1":
                                {
                                    // may be multiple
                                    int k = 0; 
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        foreach (XElement value in values)
                                        {
                                            string name = GetElementAsString(value);
                                            if (name != null && name.Length >= 4)
                                            {
                                                string st_name = name.Trim().ToLower();
                                                if (st_name != "n.a." && st_name != "none" &&
                                                    !st_name.StartsWith("see ") && !st_name.StartsWith("not avail") &&
                                                    !st_name.StartsWith("not applic") && !st_name.StartsWith("non applic") && !st_name.StartsWith("non aplic") &&
                                                    !st_name.StartsWith("no applic") && !st_name.StartsWith("no aplic") && !st_name.StartsWith("not aplic") &&
                                                    st_name != "not done" && st_name != "same as above" && st_name != "in preparation" &&
                                                    !st_name.StartsWith("non dispo") && st_name != "non fornito")
                                                {
                                                    k++;
                                                    titles.Add(new StudyTitle(sid, name, 15, "Public title", (k == 1)));
                                                }
                                            }
                                        }
                                    }
                                    break;
                                }
                            case "A.3.2":
                                {
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string topic_name = GetElementAsString(values.First());
                                        string name = topic_name.Trim().ToLower();
                                        if (!name.StartsWith("not ") && !name.StartsWith("non ") && name.Length > 2 &&
                                            name != "n/a" && name != "n.a." && name != "none" && !name.StartsWith("no ap")
                                            && !name.StartsWith("no av"))
                                        {
                                            titles.Add(new StudyTitle(sid, topic_name, 14, "Acronym or Abbreviation", false));
                                        }
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
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string isrctn_id = GetElementAsString(values.First());
                                        if (isrctn_id.ToLower().StartsWith("isrctn"))
                                        {
                                            identifiers.Add(new StudyIdentifier(sid, isrctn_id, 11, "Trial Registry ID",
                                                100126, "ISRCTN", null, null));
                                        }
                                    }
                                    break;
                                }
                            case "A.5.2":
                                {
                                    // identifier: NCT Number
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string nct_id = GetElementAsString(values.First());
                                        if (nct_id.ToLower().StartsWith("nct"))
                                        {
                                            identifiers.Add(new StudyIdentifier(sid, nct_id, 11, "Trial Registry ID",
                                                100120, "ClinicalTrials.gov", null, null));
                                        }
                                    }
                                    break;
                                }
                            case "A.5.3":
                                {
                                    // identifier: WHO UTN Number
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string who_id = GetElementAsString(values.First());
                                        if (who_id.ToLower().StartsWith("u1111"))
                                        {
                                            identifiers.Add(new StudyIdentifier(sid, who_id, 11, "Trial Registry ID",
                                                100115, "International Clinical Trials Registry Platform", null, null));
                                        }
                                    }
                                    break;
                                }
                            default:
                                {
                                    break;    // nothing left of any significance - do nothing
                                }
                        }
                    }
                }
            }

            // ensure a default and display title
            bool display_title_exists = false;
            for (int k = 0; k < titles.Count; k++)
            {
                if (titles[k].is_default)
                {
                    s.display_title = titles[k].title_text;
                    display_title_exists = true;
                    break;
                }
            }

            if (!display_title_exists)
            {
                // use a scientific title - should always be one
                for (int k = 0; k < titles.Count; k++)
                {
                    if (titles[k].title_type_id == 16)
                    {
                        titles[k].is_default = true;
                        s.display_title = titles[k].title_text;
                        display_title_exists = true;
                        break;
                    }
                }
            }

            if (!display_title_exists)
            {
                // use an acronym
                for (int k = 0; k < titles.Count; k++)
                {
                    if (titles[k].title_type_id == 14)
                    {
                        titles[k].is_default = true;
                        s.display_title = titles[k].title_text;
                        display_title_exists = true;
                        break;
                    }
                }
            }

            // add in an explanatory message... if no title
            if (!display_title_exists)
            {
                s.display_title = sid + " (No meaningful title provided)";
            }

            // study design info
            var feats = r.Element("features");
            if (feats != null)
            {
                var detail_lines = feats.Elements("DetailLine");
                if (detail_lines != null && detail_lines.Count() > 0)
                {
                    foreach (XElement dline in detail_lines)
                    { 
                        string item_code = GetElementAsString(dline.Element("item_code"));
                        switch (item_code)
                        {
                            case "E.1.1":
                                {
                                    // conditions under study
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        foreach (XElement value in values)
                                        {
                                            string name = GetElementAsString(value);
                                            topics.Add(new StudyTopic(sid, 13, "condition", name, "conditions under study"));
                                        }
                                    }
                                    break;
                                }
                            case "E.2.1":
                                {
                                    // primary objectives
                                    string objectives = "";
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string obs = GetElementAsString(values.First());
                                        if (obs != null && obs.Length >= 4 &&
                                            !obs.StartsWith("see ") && !obs.StartsWith("not "))
                                        {
                                            char[] charsToLose = { '\n', '\r', ' ' };
                                            obs.Trim(charsToLose);
                                            if (obs.StartsWith("Primary") && obs.Length > 16)
                                            {
                                                objectives = obs;
                                            }
                                            else
                                            {
                                                objectives = "Primary objectives: " + obs;
                                            }
                                        }
                                    }
                                    s.brief_description = objectives;
                                    break;
                                }
                            case "E.5.1":
                                {
                                    // primary end points
                                    string end_points = "";
                                    var values = dline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string points = GetElementAsString(values.First());
                                        if (points != null && points.Length >= 4 &&
                                            !points.StartsWith("see ") && !points.StartsWith("not "))
                                        {
                                            if (points.StartsWith("Primary") && points.Length > 16)
                                            {
                                                end_points = points;
                                            }
                                            else
                                            {
                                                end_points = "Primary endpoints: " + points;
                                            }
                                        }
                                    }

                                    if (end_points != "")
                                    {
                                        if (string.IsNullOrEmpty(s.brief_description))
                                        {
                                            s.brief_description = end_points;
                                        }
                                        else
                                        {
                                            s.brief_description += " " + end_points;
                                        }
                                    }
                                    break;

                                }
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
            }

            // eligibility
            var population = r.Element("population");
            if (population != null)
            {
                var detail_lines = feats.Elements("DetailLine");
                if (detail_lines != null && detail_lines.Count() > 0)
                {

                    bool includes_under18 = false;
                    bool includes_in_utero = false, includes_preterm = false;
                    bool includes_newborns = false, includes_infants = false;
                    bool includes_children = false, includes_ados = false;
                    bool includes_adults = false, includes_elderly = false;
                    bool includes_women = false, includes_men = false;

                    foreach (XElement dline in detail_lines)
                    {
                        string item_code = GetElementAsString(dline.Element("item_code"));
                        switch (item_code)
                        {
                            case "F.1.1":
                                {
                                    // under 18
                                    includes_under18 = true; break;
                                }
                            case "F.1.1.1":
                                {
                                    includes_in_utero = true; break;
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
                                    includes_adults = true; break;
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
                                    break;    // nothing left of any significance - do nothing
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
                            s.min_age = 18; s.min_age_units = "Years"; s.min_age_units_id = 17;
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
            }


            // for topics
            var imps = r.Element("imps");
            if (imps != null)
            {
                var imp_lines = feats.Elements("ImpLine");
                if (imp_lines != null && imp_lines.Count() > 0)
                {
                    foreach (XElement iline in imp_lines)
                    {
                        string item_code = GetElementAsString(iline.Element("item_code"));
                        switch (item_code)
                        {

                            case "D.2.1.1.1":
                                {
                                    // Trade name
                                    var values = iline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string topic_name = GetElementAsString(values.First());
                                        string name = topic_name.ToLower();
                                        if (name != "not available" && name != "n/a" && name != "na" && name != "not yet extablished")
                                        {
                                            string trade_name = topic_name.Replace(((char)174).ToString(), "");    // drop reg mark
                                            // but is it already there?
                                            bool new_topic = true;
                                            foreach (StudyTopic t in topics)
                                            {
                                                if (t.topic_value.ToLower() == trade_name.ToLower())
                                                {
                                                    new_topic = false; break;
                                                }
                                            }
                                            if (new_topic)
                                            {
                                                topics.Add(new StudyTopic(sid, 12, "chemical / agent", trade_name, "trade name"));
                                            }
                                        }
                                    }
                                    break;
                                }
                            case "D.3.1":
                                {
                                    // Product name
                                    var values = iline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string topic_name = GetElementAsString(values.First());
                                        string name = topic_name.ToLower();
                                        if (name != "not available" && name != "n/a" && name != "na" && name != "not yet extablished")
                                        {
                                            string product_name = topic_name.Replace(((char)174).ToString(), "");    // drop reg mark
                                            // but is it already there?
                                            bool new_topic = true;
                                            foreach (StudyTopic t in topics)
                                            {
                                                if (t.topic_value.ToLower() == product_name.ToLower())
                                                {
                                                    new_topic = false; break;
                                                }
                                            }
                                            if (new_topic)
                                            {
                                                topics.Add(new StudyTopic(sid, 12, "chemical / agent", product_name, "product name"));
                                            }
                                        }
                                    }
                                    break;
                                }
                            case "D.3.8":
                                {
                                    // INN
                                    var values = iline.Elements("values");
                                    if (values != null && values.Count() > 0)
                                    {
                                        string topic_name = GetElementAsString(values.First());
                                        string name = topic_name.ToLower();
                                        if (name != "not available" && name != "n/a" && name != "na" && name != "not yet extablished")
                                        {
                                            // but is it already there?
                                            bool new_topic = true;
                                            foreach (StudyTopic t in topics)
                                            {
                                                if (t.topic_value.ToLower() == name)
                                                {
                                                    new_topic = false; break;
                                                }
                                            }
                                            if (new_topic)
                                            {
                                                topics.Add(new StudyTopic(sid, 12, "chemical / agent", topic_name, "INN or proposed INN"));
                                            }
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
                                    break;    // nothing left of any significance - do nothing
                                }
                        }
                    }
                }
            }


            // public List<MeddraTerm> meddra_terms { get; set; }
            var meddra_terms = r.Element("meddra_terms");
            if (meddra_terms != null)
            {
                var terms = feats.Elements("MeddraTerm");
                if (terms != null && terms.Count() > 0)
                {
                    foreach (XElement t in terms)
                    {
                        string version = GetElementAsString(t.Element("version"));
                        string code = GetElementAsString(t.Element("code"));
                        string level = GetElementAsString(t.Element("level"));
                        string term = GetElementAsString(t.Element("term"));

                        if (!string.IsNullOrEmpty(term))
                        {
                            topics.Add(new StudyTopic(sid, 13, "condition", term, 16, code,
                                     ("MedDRA " + version + " " + level).Trim()));
                        }
                    }
                }
            }

            // not used at present
            //public string competent_authority { get; set; }

            // DATA OBJECTS and their attributes
            // initial data object is the EUCTR registry entry

            string object_display_title = s.display_title + " :: EU CTR registry entry";

            // create hash Id for the data object
            string sd_oid = hh.CreateMD5(sid + object_display_title);

            data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
                  23, "Text", 13, "Trial Registry entry", 100123, "EU Clinical Trials Register", 
                  12, download_datetime));

            // data object title is the single display title...
            object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
                                             22, "Study short name :: object type", true));


            // instance url 
            string details_url = GetElementAsString(r.Element("details_url"));
            object_instances.Add(new ObjectInstance(sd_oid, 100123, "EU Clinical Trials Register",
                        details_url, true, 35, "Web text"));

            // if there is a results url, add that in as well
            string results_url = GetElementAsString(r.Element("results_url"));
            if (!string.IsNullOrEmpty(results_url))
            {
                object_display_title = s.display_title + " :: EU CTR results entry";
                sd_oid = hh.CreateMD5(sid + object_display_title);

                data_objects.Add(new DataObject(sd_oid, sid, object_display_title, s.study_start_year,
                      23, "Text", 28, "Trial registry results summary", 100123, 
                      "EU Clinical Trials Register", 12, download_datetime));

                // data object title is the single display title...
                object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
                                                         22, "Study short name :: object type", true));

                // instance url 
                object_instances.Add(new ObjectInstance(sd_oid,  100123, "EU Clinical Trials Register",
                            results_url, true, 36, "Web text with download"));
            }


            // Check brief description for html
            // after getting rid of any sup / subs, divs and spans.

            s.brief_description = mh.replace_tags(s.brief_description);
            s.bd_contains_html = mh.check_for_tags(s.brief_description);

            s.identifiers = identifiers;
            s.titles = titles;
            s.contributors = contributors;
            s.topics = topics;
            s.features = features;

            s.data_objects = data_objects;
            s.object_titles = object_titles;
            s.object_instances = object_instances;

            return s;

        }
                    
        public void StoreData(Study s, string db_conn)
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

            _storage_repo.StoreStudy(dbs, db_conn);

            StudyCopyHelpers sch = new StudyCopyHelpers();
            ObjectCopyHelpers och = new ObjectCopyHelpers();

            if (s.identifiers.Count > 0)
            {
                _storage_repo.StoreStudyIdentifiers(sch.study_ids_helper, s.identifiers, db_conn);
            }

            if (s.titles.Count > 0)
            {
                _storage_repo.StoreStudyTitles(sch.study_titles_helper, s.titles, db_conn);
            }

            if (s.contributors.Count > 0)
            {
                _storage_repo.StoreStudyContributors(sch.study_contributors_helper, s.contributors, db_conn);
            }

            if (s.topics.Count > 0)
            {
                _storage_repo.StoreStudyTopics(sch.study_topics_helper, s.topics, db_conn);
            }

            if (s.features.Count > 0)
            {
                _storage_repo.StoreStudyFeatures(sch.study_features_helper, s.features, db_conn);
            }

            if (s.data_objects.Count > 0)
            {
                _storage_repo.StoreDataObjects(och.data_objects_helper, s.data_objects, db_conn);
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
