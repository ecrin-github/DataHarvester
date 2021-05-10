using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using Serilog;

namespace DataHarvester.yoda
{
    public class YodaProcessor : IStudyProcessor
    {
        IStorageDataLayer _storage_repo;
        IMonitorDataLayer _mon_repo;
        ILogger _logger;

        public YodaProcessor(IStorageDataLayer storage_repo, IMonitorDataLayer mon_repo, ILogger logger)
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
            List<StudyReference> study_references = new List<StudyReference>();
            List<StudyContributor> study_contributors = new List<StudyContributor>();
            List<StudyTopic> study_topics = new List<StudyTopic>();


            List<DataObject> data_objects = new List<DataObject>();
            List<ObjectDataset> object_datasets = new List<ObjectDataset>();
            List<ObjectTitle> data_object_titles = new List<ObjectTitle>();
            List<ObjectInstance> data_object_instances = new List<ObjectInstance>();

            string access_details = "The YODA Project will require that requestors provide basic information about the Principal Investigator, Key Personnel, and the ";
            access_details += "project Research Proposal, including a scientific abstract and research methods.The YODA Project will review proposals to ensure that: ";
            access_details += "1) the scientific purpose is clearly described; 2) the data requested will be used to enhance scientific and/or medical knowledge; and ";
            access_details += "3) the proposed research can be reasonably addressed using the requested data.";

            StringHelpers sh = new StringHelpers(_logger, _mon_repo);
            MD5Helpers hh = new MD5Helpers();
            HtmlHelpers mh = new HtmlHelpers(_logger);

            // First convert the XML document to a Linq XML Document.

            XDocument xDoc = XDocument.Load(new XmlNodeReader(d));

            // Obtain the main top level elements of the registry entry.
            // In most cases study will have already been registered in CGT.
            XElement r = xDoc.Root;
            
            string sid = GetElementAsString(r.Element("sd_sid"));
            s.sd_sid = sid;
            s.datetime_of_data_fetch = download_datetime;

            string yoda_title = GetElementAsString(r.Element("yoda_title"));
            string display_title = GetElementAsString(r.Element("display_title"));
            bool is_yoda_only = GetElementAsBool(r.Element("sd_sid")); 
            string remote_url = GetElementAsString(r.Element("remote_url"));
            string therapaeutic_area = GetElementAsString(r.Element("therapaeutic_area"));
            string product_class = GetElementAsString(r.Element("product_class"));
            string sponsor_protocol_id = GetElementAsString(r.Element("sponsor_protocol_id"));
            string data_partner = GetElementAsString(r.Element("data_partner"));
            string conditions_studied = GetElementAsString(r.Element("conditions_studied"));
            string mean_age = GetElementAsString(r.Element("mean_age"));
            string last_revised_date = GetElementAsString(r.Element("last_revised_date")); ;

            if (yoda_title.Contains("<"))
            {
                yoda_title = mh.replace_tags(yoda_title);
                yoda_title = mh.strip_tags(yoda_title);
            }

            if (display_title != null)
            {
                s.display_title = display_title;
            }
            else
            {
                s.display_title = yoda_title;
            }

            // No brief description available 
            // for Yoda records
            
            s.study_status_id = 21;
            s.study_status = "Completed";  // assumption for entry onto web site

            // previously obtained from the ctg or isrctn entry
            //s.study_type_id = st.type_id;
            //if (st.type_id == 11)
            //{
            //    s.study_type = "Interventional";
            //}
            //else if (st.type_id == 12)
            //{
            //    s.study_type = "Observational";
            //}

            // study type only really relevant for non registered studies (others will  
            // have type identified in registered study entry

            string enrolment = GetElementAsString(r.Element("enrolment"));
            string percent_female = GetElementAsString(r.Element("percent_female"));
            string percent_male = GetElementAsString(r.Element("percent_male"));

            if (Int32.TryParse(enrolment, out int enrolment_number))
            {
                s.study_enrolment = enrolment_number;
            }

            if (!string.IsNullOrEmpty(percent_female) && percent_female != "N/A")
            {
                if (percent_female.EndsWith("%"))
                {
                    percent_female = percent_female.Substring(0, percent_female.Length - 1);
                }

                if (Single.TryParse(percent_female, out float female_percentage))
                {
                    if (female_percentage == 0)
                    {
                        s.study_gender_elig_id = 910;
                        s.study_gender_elig = "Male";
                    }
                    else if (female_percentage == 100)
                    {
                        s.study_gender_elig_id = 905;
                        s.study_gender_elig = "Female";
                    }
                    else
                    {
                        s.study_gender_elig_id = 900;
                        s.study_gender_elig = "All";
                    }
                }
            }
            else
            {
                s.study_gender_elig_id = 915;
                s.study_gender_elig = "Not provided";
            }

            // transfer title data
            // Normally just one - the 'yoda title'

           //if (st.study_titles.Count > 0)
            //{
           //    foreach(Title t in st.study_titles)
             //   {
             //       study_titles.Add(new StudyTitle(sid, t.title_text, t.title_type_id, t.title_type, t.is_default, t.comments));
             //   }
            //}

            // transfer identifier data
            // Normally a protocol id will be the only addition (may be a duplicate of one already in the system)
            XElement sids = r.Element("study_identifiers");
            if (sids != null)
            {
                var study_idents = sids.Elements("Identifier");
                if (study_idents != null && study_idents.Count() > 0)
                {
                    foreach (XElement i in study_idents)
                    {
                        string identifier_value = GetElementAsString(r.Element("identifier_value"));
                        int? identifier_type_id = GetElementAsInt(r.Element("identifier_type_id"));
                        string identifier_type = GetElementAsString(r.Element("identifier_type"));
                        int? identifier_org_id = GetElementAsInt(r.Element("identifier_org_id"));
                        string identifier_org = GetElementAsString(r.Element("identifier_org"));
                        study_identifiers.Add(new StudyIdentifier(sid, identifier_value, identifier_type_id, identifier_type,
                                                          identifier_org_id, identifier_org));
                    }
                }
            }

            // study contributors
            // only sponsor knowm, and only relevant for non registered studies (others will  
            // have type identified in registered study entry).
            int? sponsor_org_id; string sponsor_org;
            int? sponsor_id = GetElementAsInt(r.Element("sponsor_id"));
            string sponsor = GetElementAsString(r.Element("sponsor"));
            if (!string.IsNullOrEmpty(sponsor))
            {
                sponsor_org_id = sponsor_id;
                sponsor_org = sh.TidyOrgName(sponsor, sid);
            }
            else
            {
                sponsor_org_id = null; 
                sponsor_org = "No organisation name provided in source data";
            }

            if (is_yoda_only)
            {
                study_contributors.Add(new StudyContributor(sid, 54, "Study Sponsor", sponsor_org_id, sponsor_org, null, null));
            }

            // study topics
            string compound_generic_name = GetElementAsString(r.Element("compound_generic_name"));
            string compound_product_name = GetElementAsString(r.Element("compound_product_name"));
            if (!string.IsNullOrEmpty(compound_generic_name))
            {
                study_topics.Add(new StudyTopic(sid, 12, "chemical / agent", compound_generic_name, "generic name"));
            }

            if (!string.IsNullOrEmpty(compound_product_name))
            {
                string product_name = compound_product_name.Replace(((char)174).ToString(), "");    // drop reg mark
                product_name = product_name.Replace("   ", " ").Replace("  ", " ").Trim();

                // see if already exists
                bool add_product = true;
                foreach(StudyTopic t in study_topics)
                {
                    if (product_name.ToLower() == t.topic_value.ToLower())
                    {
                        add_product = false;
                        break;
                    }
                }
                if (add_product)
                {
                    product_name = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(product_name.ToLower());
                    study_topics.Add(new StudyTopic(sid, 12, "chemical / agent", product_name, "trade name"));
                }
            }

            if (!string.IsNullOrEmpty(conditions_studied))
            {
                study_topics.Add(new StudyTopic(sid, 13, "condition", conditions_studied));
            }

            // create study references (pmids)
            XElement refs = r.Element("study_references");
            if (refs != null)
            {
                var study_refs = refs.Elements("Reference");
                if (study_refs != null && study_refs.Count() > 0)
                {
                    foreach (XElement sr in study_refs)
                    {
                        string pmid = GetElementAsString(sr.Element("pmid"));
                        string primary_citation_link = GetElementAsString(sr.Element("primary_citation_link"));
                        // normally only 1 if there is one there at all 
                        study_references.Add(new StudyReference(sid, pmid, primary_citation_link, "", ""));
                    }
                }
            }

        
            // data objects...

            string name_base = s.display_title;  // will be the NCT display title in most cases, otherwise the yoda title
            
            // do the yoda web page itself first...
            string object_display_title = name_base + " :: " + "Yoda web page";

            // create hash Id for the data object
            string sd_oid = hh.CreateMD5(sid + object_display_title);

            data_objects.Add(new DataObject(sd_oid, sid, object_display_title, null, 23, "Text", 38, "Study Overview",
                              101901, "Yoda", 12, download_datetime));
            data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22,
                            "Study short name :: object type", true));
            data_object_instances.Add(new ObjectInstance(sd_oid, 101901, "Yoda",
                                remote_url, true, 35, "Web text"));

            // then for each supp doc...
            XElement sds = r.Element("supp_docs");
            if (sds != null)
            {
                var supp_docs = sds.Elements("SuppDoc");
                if (supp_docs != null && supp_docs.Count() > 0)
                {
                    foreach (XElement sd in supp_docs)
                    {
                        // get object_type
                        int object_class_id = 0; string object_class = "";
                        int object_type_id = 0; string object_type = "";
                        string doc_name = GetElementAsString(sd.Element("doc_name"));
                        string comment = GetElementAsString(sd.Element("comment"));
                        string url = GetElementAsString(sd.Element("url"));

                        switch (doc_name)
                        {
                            case "Collected Datasets":
                                {
                                    object_type_id = 80;
                                    object_type = "Individual Participant Data";
                                    object_class_id = 14; object_class = "Datasets";
                                    break;
                                }
                            case "Data Definition Specification":
                                {
                                    object_type_id = 31;
                                    object_type = "Data Dictionary";
                                    object_class_id = 23; object_class = "Text";
                                    break;
                                }
                            case "Analysis Datasets":
                                {
                                    object_type_id = 51;
                                    object_type = "IPD final analysis datasets (full study population)";
                                    object_class_id = 14; object_class = "Datasets";
                                    break;
                                }
                            case "CSR Summary":
                                {
                                    object_type_id = 79;
                                    object_type = "CSR Summary";
                                    object_class_id = 23; object_class = "Text";
                                    break;
                                }
                            case "Annotated Case Report Form":
                                {
                                    object_type_id = 30;
                                    object_type = "Annotated Data Collection Forms";
                                    object_class_id = 23; object_class = "Text";
                                    break;
                                }
                            case "Statistical Analysis Plan":
                                {
                                    object_type_id = 22;
                                    object_type = "Statistical analysis plan";
                                    object_class_id = 23; object_class = "Text";
                                    break;
                                }
                            case "Protocol with Amendments":
                                {
                                    object_type_id = 11;
                                    object_type = "Study Protocol";
                                    object_class_id = 23; object_class = "Text";
                                    break;
                                }
                            case "Clinical Study Report":
                                {
                                    object_type_id = 26;
                                    object_type = "Clinical Study Report";
                                    object_class_id = 23; object_class = "Text";
                                    break;
                                }
                        }

                        object_display_title = name_base + " :: " + object_type;
                        sd_oid = hh.CreateMD5(sid + object_display_title);

                        if (comment == "Available now")
                        {
                            data_objects.Add(new DataObject(sd_oid, sid, object_display_title, null, object_class_id, object_class, object_type_id, object_type,
                                            101901, "Yoda", 11, download_datetime));
                            data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22, "Study short name :: object type", true));

                            // create instance as resource exists
                            // get file type from link if possible
                            int resource_type_id = 0; string resource_type = "";
                            if (url.ToLower().EndsWith(".pdf"))
                            {
                                resource_type_id = 11;
                                resource_type = "PDF";
                            }
                            else if (url.ToLower().EndsWith(".xls"))
                            {
                                resource_type_id = 18;
                                resource_type = "Excel Spreadsheet(s)";
                            }
                            else
                            {
                                resource_type_id = 0;
                                resource_type = "Not yet known";
                            }
                            data_object_instances.Add(new ObjectInstance(sd_oid, 101901, "Yoda", url, true, resource_type_id, resource_type));
                        }
                        else
                        {
                            DateTime date_access_url_checked = new DateTime(2020, 9, 23);
                            data_objects.Add(new DataObject(sd_oid, sid, object_display_title, null, object_class_id, object_class, object_type_id, object_type,
                                            101901, "Yoda", 17, "Case by case download", access_details,
                                            "https://yoda.yale.edu/how-request-data", date_access_url_checked, download_datetime));
                            data_object_titles.Add(new ObjectTitle(sd_oid, object_display_title, 22, "Study short name :: object type", true));
                        }

                        // for datasets also add dataset properties - even if they are largely unknown
                        if (object_type_id == 80)
                        {
                            object_datasets.Add(new ObjectDataset(sd_oid, 0, "Not known", "",
                                                      2, "De-identification applied",
                                                      "Yoda states that '...researchers will be granted access to participant-level study data that are devoid of personally identifiable information; current best guidelines for de-identification of data will be used.'",
                                                      0, "Not known", ""));
                        }
                    }
                }
            }

            // add in the study properties
            s.identifiers = study_identifiers;
            s.titles = study_titles;
            s.references = study_references;
            s.contributors = study_contributors;
            s.topics = study_topics;

            s.data_objects = data_objects;
            s.object_datasets = object_datasets;
            s.object_titles = data_object_titles;
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

            if (s.references.Count > 0)
            {
                _storage_repo.StoreStudyReferences(sch.study_references_helper, s.references, db_conn);
            }

            if (s.contributors.Count > 0)
            {
                _storage_repo.StoreStudyContributors(sch.study_contributors_helper, s.contributors, db_conn);
            }

            if (s.topics.Count > 0)
            {
                _storage_repo.StoreStudyTopics(sch.study_topics_helper, s.topics, db_conn);
            }

            // store data objects and dataset properties
            if (s.data_objects.Count > 0)
            {
                _storage_repo.StoreDataObjects(och.data_objects_helper, s.data_objects, db_conn);
            }

            if (s.object_datasets.Count > 0)
            {
                _storage_repo.StoreDatasetProperties(och.object_datasets_helper, s.object_datasets, db_conn);
            }

            // store data object attributes
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






