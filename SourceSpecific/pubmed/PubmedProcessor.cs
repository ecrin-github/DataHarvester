﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace DataHarvester.pubmed
{

    public class PubmedProcessor
    {
        // The main data extraction function.
        // Its inputs include a single XML document representing the citation
        // as well as the currrent PMID and a refrence to the data layer.

        public CitationObject ProcessData(DataLayer repo, string sd_oid, XmlDocument d)
        {

            // First convert the XML document to a Linq XML Document.

            XDocument xDoc = XDocument.Load(new XmlNodeReader(d));

            // obtain the main top level elements of the citation.

            XElement pubmedArticle = xDoc.Root;
            XElement citation = pubmedArticle.Element("MedlineCitation");
            XElement pubmed = pubmedArticle.Element("PubmedData");
            XElement article = citation.Element("Article");
            XElement journal = article.Element("Journal");
            XElement JournalInfo = citation.Element("MedlineJournalInfo");

            // Establish citation object and list structures to receive data
            // Each of the lists corresponds to one of the repeating compomnents of the 
            // citation object - they will be added to it at the end of the extraction.

            CitationObject c = new CitationObject();

            List<ObjectDate> dates = new List<ObjectDate>();
            List<ObjectTitle> titles = new List<ObjectTitle>();
            List<ObjectIdentifier> ids = new List<ObjectIdentifier>();
            List<ObjectTopic> topics = new List<ObjectTopic>();
            List<ObjectPublicationType> pubtypes = new List<ObjectPublicationType>();
            List<ObjectDescription> descriptions = new List<ObjectDescription>();
            List<ObjectInstance> instances = new List<ObjectInstance>();
            List<ObjectLanguage> object_languages = new List<ObjectLanguage>();
            List<ObjectContributor> contributors = new List<ObjectContributor>();

            string author_string = "";
            string art_title = "";
            string journal_title = "";
            string journal_source = "";



            #region Header

            // Identify the PMID as the source data Id (sd_id), and also construct and add 
            // this to the 'other identifiers' list ('other' because it is not a doi).
            // The date applied may or may not be available later.

            c.sd_oid = sd_oid;
            ids.Add(new ObjectIdentifier(sd_oid, 16, "PMID", sd_oid, 100133, "National Library of Medicine"));

            // Set the PMID entry as an object instance (type id 3 = abstract, resource 40 = Web text journal abstract), add to the instances list.

            ObjectInstance inst = new ObjectInstance
            {
                sd_oid = c.sd_oid,
                instance_type_id = 3,
                instance_type = "Article abstract",
                url = "https://www.ncbi.nlm.nih.gov/pubmed/" + sd_oid,
                url_accessible = true,
                repository_org_id = 100133,
                repository_org = "National Library of Medicine",
                resource_type_id = 40,
                resource_type = "Web text journal abstract"
            };
            instances.Add(inst);

            // Can assume there is always a PMID element ... (if not the 
            // original data search for this citation would not have worked).
            // Get the associated version and note if it is not present or 
            // not in the right format - 
            // these exceptions appear to be very rare if they occur at all.

            XElement p = citation.Element("PMID");

            string pmidVersion = GetAttributeAsString(p.Attribute("Version"));
            if (pmidVersion != null)
            {
                if (Int32.TryParse(pmidVersion, out int res))
                {
                    c.sd_oid_version = res;
                }
                else
                {
                    repo.StoreExtractionNote(sd_oid, 16, "PMID version not an integer");
                }
            }
            else
            {
                repo.StoreExtractionNote(sd_oid, 16, "No PMID version attribute found");
            }

            // Obtain and store the citation status.

            c.status = GetAttributeAsString(citation.Attribute("Status"));

            // Version and version_date hardly ever present
            // if they do occur put them in as an extraction note.

            string version_id = GetAttributeAsString(citation.Attribute("VersionID"));
            string version_date = GetAttributeAsString(citation.Attribute("VersionDate"));
            if (version_id != null)
            {
                string qText = "A version attribute (" + version_id + ") found for this citation!";
                repo.StoreExtractionNote(sd_oid, 15, qText);
            }
            if (version_date != null)
            {
                string qText = "A version date attribute (" + version_date + ") found for this citation!";
                repo.StoreExtractionNote(sd_oid, 15, qText);
            }

            #endregion



            #region Basic Properties

            // Obtain and store the article publication model

            c.pub_model = GetAttributeAsString(article.Attribute("PubModel"));


            // Obtain and store (in the languages list) the article's language(s) - 
            // get these early as may be needed by title extraction code below.

            var languages = article.Elements("Language");
            if (languages.Count() > 0)
            {
                c.LanguageList = languages.Select(g => GetElementAsString(g)).ToList();
                foreach (string g in languages)
                {
                    object_languages.Add(new ObjectLanguage(sd_oid, g));
                }
            }


            // Obtain article title(s).
            // Usually just a single title present in English, but may be an additional
            // title in the 'vernacular', with a translation in English.
            // Translated titles are in square brackets and may be followed by a comment
            // in parantheses. 
            // First set up the set of required variables.

            bool article_title_present = true;
            bool vernacular_title_present = false;
            string atitle = "";
            string vtitle = "";
            string vlang_code = "";
            bool vernacular_title_expected = false;

            // get some basic journal information, as this is useful for helping to 
            // determine the country of origin, and for identifying the publisher,
            // as well as later (in creating citation string). The journal name
            // and issn numbers for electronic and / or paper versions are obtained.

            if (journal != null)
            {
                string journalTitle = GetElementAsString(journal.Element("Title"));
                c.journal_title = journalTitle;
                IEnumerable<XElement> ISSNs = journal.Elements("ISSN");
                if (ISSNs.Count() > 0)
                {
                    foreach (XElement issn_id in ISSNs)
                    {
                        // Note the need to clean pissn / eissn numbers to a standard format.

                        string ISSN_type = GetAttributeAsString(issn_id.Attribute("IssnType"));
                        if (ISSN_type == "Print")
                        {
                            string pissn = GetElementAsString(issn_id);
                            if (pissn.Length == 9 && pissn[4] == '-')
                            {
                                pissn = pissn.Substring(0, 4) + pissn.Substring(5, 4);
                            }
                            c.pissn = pissn;
                        }
                        if (ISSN_type == "Electronic")
                        {
                            string eissn = GetElementAsString(issn_id);
                            if (eissn.Length == 9 && eissn[4] == '-')
                            {
                                eissn = eissn.Substring(0, 4) + eissn.Substring(5, 4);
                            }
                            c.eissn = eissn;
                        }
                    }
                }
            }

            // Get the main article title and check for any html. Log any exception conditions.
            // Can't use the standard helper methods here as these strip out contained html,
            // therefore use an XML reader to obtain the InnerXML of the Title element.

            XElement article_title = article.Element("ArticleTitle");
            if (article_title != null)
            {
                var areader = article_title.CreateReader();
                areader.MoveToContent();
                atitle = areader.ReadInnerXml().Trim();
                if (atitle != "")
                {
                    if (atitle.Contains("<"))
                    {   
                        // Log this in extraction_notes and strip tags apart from super and subscripts.
                        string qText = "The article title may include embedded html (" + atitle + ")";
                        repo.StoreExtractionNote(sd_oid, 17, qText);

                        atitle = HtmlHelpers.replace_tags(atitle);
                        atitle = HtmlHelpers.strip_tags(atitle);
                    }
                }
                else
                {
                    article_title_present = false;
                    string qText = "The citation has an empty article title element";
                    repo.StoreExtractionNote(sd_oid, 28, qText);
                }
            }
            else
            {
                article_title_present = false;
                string qText = "The citation does not have an article title element";
                repo.StoreExtractionNote(sd_oid, 28, qText);
            }


            // Get the vernacular title if there is one and characterise it
            // in a similar way, noting any html.

            XElement vernacular_title = article.Element("VernacularTitle");

            if (vernacular_title != null)
            {
                vernacular_title_present = true;
                var vreader = vernacular_title.CreateReader();
                vreader.MoveToContent();
                vtitle = vreader.ReadInnerXml().Trim();

                if (vtitle != "")
                {
                    if (atitle.Contains("<"))
                    {
                        // Log this in extraction_notes and strip tags apart from super and subscripts.
                        string qText = "The vernacular title may include embedded html (" + vtitle + ")";
                        repo.StoreExtractionNote(sd_oid, 17, qText);

                        vtitle = HtmlHelpers.replace_tags(vtitle);
                        vtitle = HtmlHelpers.strip_tags(vtitle);
                    }

                    // Try and get vernacular code language - not explicitly given so
                    // all methods imperfect but seem to work in most situations so far.

                    foreach (string s in c.LanguageList)
                    {
                        if (s != "eng")
                        {
                            vlang_code = s; // though may not be first non-english language if there are more than one
                            break;
                        }
                    }

                    if (vlang_code == "")
                    {
                        // Check journal country of publication - suggests a reasonable guess!

                        if (JournalInfo != null)
                        {
                            string country = GetElementAsString(JournalInfo.Element("Country"));
                            switch (country)
                            {
                                case "Canada": vlang_code = "fre"; break;
                                case "France": vlang_code = "fre"; break;
                                case "Germany": vlang_code = "ger"; break;
                                case "Spain": vlang_code = "spa"; break;
                                case "Mexico": vlang_code = "spa"; break;
                                case "Argentina": vlang_code = "spa"; break;
                                case "Chile": vlang_code = "spa"; break;
                                case "Peru": vlang_code = "spa"; break;
                                case "Portugal": vlang_code = "por"; break;
                                case "Brazil": vlang_code = "por"; break;
                                case "Italy": vlang_code = "ita"; break;
                                case "Russia": vlang_code = "rus"; break;
                                case "Turkey": vlang_code = "tur"; break;
                                    // need to add more...
                            }
                        }
                    }

                    if (vlang_code == "")
                    {
                        // Some Canadian journals are published in the US
                        // and often have a French alternate title.

                        if (journal_title.Contains("Canada") || journal_title.Contains("Canadian"))
                        {
                            vlang_code = "fre";
                        }

                    }

                    // But check the vernaculat title is not the same as the article title - can happen 
                    // very rarely and if it is the case the vernacular title should be ignored.

                    if (vtitle == atitle)
                    {
                        vernacular_title_present = false;
                        string qText = "The article and vernacular titles seem identical";
                        repo.StoreExtractionNote(sd_oid, 29, qText);
                    }
                }
            }


            // Having established whether a non-null article title exists, and the presence or
            // not of a vernaculat title in a particular language, this section examines
            // the possible relationship between the two.

            if (article_title_present)
            {
                // First check if it starts with a square bracket.
                // This indicates a translation of a title originally not in English.
                // There should therefore be a vernacular title also.
                // Get the English title and any comments in brackets following the square brackets.

                if (atitle.StartsWith("["))
                {
                    string poss_comment = "";

                    // Strip off any final full stops from brackets, parenthesis, to make testing below easier.

                    if (atitle.EndsWith("].") || atitle.EndsWith(")."))
                    {
                        atitle = atitle.Substring(0, atitle.Length - 1);
                    }

                    if (atitle.EndsWith("]"))
                    {
                        // No supplementary comment (This is almost always the case).
                        // Get the article title without brackets and expect a vernacular title.

                        atitle = atitle.Substring(1, atitle.Length - 2);  // remove the square brackets at each end
                        vernacular_title_expected = true;
                    }
                    else if (atitle.EndsWith(")"))
                    {
                        // Work back from the end to get the matching left parenthesis.
                        // Because the comment may itself contain parantheses necessary to
                        // match the correct left bracket.
                        // Obtain comment, and article title, and log if this seems impossible to do.

                        int bracket_count = 1;
                        for (int i = atitle.Length - 2; i >= 0; i--)
                        {
                            if (atitle[i] == '(') bracket_count--;
                            if (atitle[i] == ')') bracket_count++;
                            if (bracket_count == 0)
                            {
                                poss_comment = atitle.Substring(i + 1, atitle.Length - i - 2);
                                atitle = atitle.Substring(1, i - 2);
                                vernacular_title_expected = true;
                                break;
                            }
                        }
                        if (bracket_count > 0)
                        {
                            string qText = "The title starts with '[', end with ')', but unable to match parentheses. Title = " + atitle;
                            repo.StoreExtractionNote(sd_oid, 18, qText);
                            vernacular_title_expected = false;
                        }
                    }
                    else
                    {
                        // Log if a square bracket at the start is not matched by an ending bracket or paranthesis.

                        vernacular_title_expected = false;
                        string qText = "The title starts with a '[' but there is no matching ']' or ')' at the end of the title. Title = " + atitle;
                        repo.StoreExtractionNote(sd_oid, 18, qText);
                    }

                    // Store the title(s) - square brackets being present.

                    if (!vernacular_title_present)
                    {
                        if (vernacular_title_expected)
                        {
                            // Something odd, no vernacular title but one was expected.

                            string qText = "There is no vernacular title but the article title appears to be translated";
                            repo.StoreExtractionNote(sd_oid, 18, qText);
                        }

                        // Add the article title, without the brackets and with any comments - as the only title present it becomes the default.

                        titles.Add(new ObjectTitle(c.sd_oid, atitle, 19, "Journal article title", "eng", 11, true, poss_comment));
                    }
                    else
                    {
                        // Both titles are present, add them both, with the vernacular title as the default.

                        titles.Add(new ObjectTitle(c.sd_oid, atitle, 19, "Journal article title", "eng", 12, false, poss_comment));

                        titles.Add(new ObjectTitle(c.sd_oid, vtitle, 19, "Journal article title", vlang_code, 21, true, ""));
                    }

                }
                else
                {
                    // No square brackets - should be a single article title, but sometimes not the case...
                    // for example Canadian journals may have both English and French titles even if everything else is in English.

                    if (vernacular_title_present)
                    {
                        // Possibly something odd, vernacular title but no indication of translation in article title.

                        string qText = "There is a vernacular title but the article title does not indicate it is translated";
                        repo.StoreExtractionNote(sd_oid, 18, qText);

                        // Add the vernacular title, will not be the default in this case.

                        titles.Add(new ObjectTitle(sd_oid, vtitle, 19, "Journal article title", vlang_code, 21, false, ""));
                    }

                    // The most common, default situation - simply add only title as the default title record in English.

                    titles.Add(new ObjectTitle(sd_oid, atitle, 19, "Journal article title", "eng", 11, true, ""));
                }
            }
            else
            {
                // No article title at all, if there is a vernacular title use that as the default.

                if (vernacular_title_present)
                {
                    titles.Add(new ObjectTitle(sd_oid, vtitle, 19, "Journal article title", vlang_code, 21, true, ""));
                }
            }

            // Make the art_title variable (will be used within the display title) the default title.

            if (titles.Count > 0)
            {
                foreach (ObjectTitle t in titles)
                {
                    if ((bool)t.is_default)
                    {
                        art_title = t.title_text;
                        break;
                    }
                }
            }


            // Obtain and store publication status.

            c.publication_status = GetElementAsString(pubmed.Element("PublicationStatus"));

            // Obtain any article databank list - to identify links to
            // registries and / or gene or protein databases. Each distinct bank
            // is given an integer number (n) which is used within the 
            // DB_Accession_Number records.

            XElement databanklist = article.Element("DataBankList");
            List<ObjectDBAccessionNumber> acc_numbers = null;

            if (databanklist != null)
            {
                int n = 0;
                foreach (XElement db in databanklist.Elements("DataBank"))
                {
                    string bnkname = GetElementAsString(db.Element("DataBankName"));
                    n++;

                    if (db.Element("AccessionNumberList") != null)
                    {
                        XElement accList = db.Element("AccessionNumberList");

                        // Get the accession numbers for this list, for this databank.
                        // Add each to the DB_Acession_Number list.

                        acc_numbers = accList.Elements("AccessionNumber")
                                .Select(a => new ObjectDBAccessionNumber
                                {
                                    sd_oid = c.sd_oid,
                                    bank_id = n,
                                    bank_name = bnkname,
                                    accession_number = GetElementAsString(a)
                                }).ToList();

                        // Extra work if the databank is clinicaltrials.gov
                        // and more than 1 link - in this case log it.

                        if (bnkname.ToLower() == "clinicaltrials.gov")
                        {
                            int num_nct_links = acc_numbers.Count();
                            if (num_nct_links > 1)
                            {
                                string qText = "This study has " + num_nct_links.ToString() + " clinicalTrials.gov ids. ";
                                qText += "All should be stored in the accession numbers table.";
                                repo.StoreExtractionNote(sd_oid, 13, qText);
                            }
                        }
                    }
                }

                // Add the accession numbers to the Citation Object.
                c.accession_numbers = acc_numbers;
            }

            #endregion



            #region Dates

            string publication_date_string = null;    // Used to summarise the date(s) in the display title.

            // Get the publication date.
            // If non standard transfer direct to the date as a string,
            // If standard process to a standard date format.

            var pub_date = article.Element("Journal")?
                                .Element("JournalIssue")?
                                .Element("PubDate");

            if (pub_date != null)
            {
                ObjectDate publication_date = null;
                if (pub_date.Element("MedlineDate") != null)
                {
                    // A string 'Medline' date, a range or a non-standard date.
                    // ProcessMedlineDate is a helper function that tries to 
                    // split any range.

                    string date_string = pub_date.Element("MedlineDate").Value;
                    publication_date = DateHelpers.ProcessMedlineDate(sd_oid, date_string, 12, "Available");
                }
                else
                {
                    // An 'ordinary' composite Y, M, D date.
                    // ProcessDate is a helper function that splits the date components, 
                    //  identifies partial dates, and creates the date as a string.

                    publication_date = DateHelpers.ProcessDate(c.sd_oid, pub_date, 12, "Available");
                }
                dates.Add(publication_date);
                c.publication_year = publication_date.start_year;
                publication_date_string = publication_date.date_as_string;
            }

            // The dates of the citation itself (not the article).

            var date_citation_created = citation.Element("DateCreated");
            if (date_citation_created != null)
            {
                dates.Add(DateHelpers.ProcessDate(sd_oid, date_citation_created, 52, "Pubmed citation created"));
            }

            var date_citation_revised = citation.Element("DateRevised");
            if (date_citation_revised != null)
            {
                dates.Add(DateHelpers.ProcessDate(sd_oid, date_citation_revised, 53, "Pubmed citation revised"));
            }

            var date_citation_completed = citation.Element("DateCompleted");
            if (date_citation_completed != null)
            {
                dates.Add(DateHelpers.ProcessDate(sd_oid, date_citation_completed, 54, "Pubmed citation completed"));
            }


            // Article date - should be used only for electronic publication.

            string electronic_date_string = null;
            var artdates = article.Elements("ArticleDate");
            if (artdates.Count() > 0)
            {
                string date_type = null;
                IEnumerable<XElement> article_dates = article.Elements("ArticleDate");
                foreach (XElement e in article_dates)
                {
                    date_type = GetAttributeAsString(e.Attribute("DateType"));

                    if (date_type != null)
                    {
                        if (date_type.ToLower() == "electronic")
                        {
                            // = epublish, type id 55
                            ObjectDate electronic_date = DateHelpers.ProcessDate(sd_oid, e, 55, "Epublish");
                            dates.Add(electronic_date);
                            electronic_date_string = electronic_date.date_as_string;
                        }
                        else
                        {
                            string qText = "Unexpected date type (" + date_type + ") found in an article date element";
                            repo.StoreExtractionNote(sd_oid, 19, qText);

                        }
                    }
                }
            }


            // Process History element with possible list of Pubmed dates.

            XElement history = pubmed.Element("History");
            if (history != null)
            {
                IEnumerable<XElement> pubmed_dates = history.Elements("PubMedPubDate");
                if (pubmed_dates.Count() > 0)
                {
                    string pub_status = null;
                    int date_type = 0;
                    string date_type_name = null;
                    foreach (XElement e in pubmed_dates)
                    {
                        pub_status = GetAttributeAsString(e.Attribute("PubStatus"));
                        if (pub_status != null)
                        {
                            // get date_type
                            switch (pub_status.ToLower())
                            {
                                case "received": date_type = 17; date_type_name = "Submitted"; break;
                                case "accepted": date_type = 11; date_type_name = "Accepted"; break;
                                case "epublish":
                                    {
                                        // an epublish date may already be in from article date
                                        // DateNotPresent is a helper function that indicates if a date 
                                        // of a particular type has already been provided or not.

                                        int? year = GetElementAsInt(e.Element("Year"));
                                        int? month = GetElementAsInt(e.Element("Month"));
                                        int? day = GetElementAsInt(e.Element("Day"));
                                        if (IdentifierHelpers.DateNotPresent(dates, 55, year, month, day))
                                        {
                                            date_type = 55;
                                            date_type_name = "Epublish";
                                        }
                                        break;
                                    }
                                case "ppublish": date_type = 56; date_type_name = "Ppublish"; break;
                                case "revised": date_type = 57; date_type_name = "Revised"; break;
                                case "aheadofprint": date_type = 58; date_type_name = "Ahead of print publication"; break;
                                case "retracted": date_type = 59; date_type_name = "Retracted"; break;
                                case "ecollection": date_type = 60; date_type_name = "Added to eCollection"; break;
                                case "pmc": date_type = 61; date_type_name = "Added to PMC"; break;
                                case "pubmed": date_type = 62; date_type_name = "Added to Pubmed"; break;
                                case "medline": date_type = 63; date_type_name = "Added to Medline"; break;
                                case "entrez": date_type = 65; date_type_name = "Added to entrez"; break;
                                case "pmc-release": date_type = 64; date_type_name = "PMC embargo release"; break;
                                default:
                                    {
                                        date_type = 0;
                                        string qText = "A unexpexted status (" + pub_status + ") found a date in the history section";
                                        repo.StoreExtractionNote(sd_oid, 20, qText);
                                        break;
                                    }
                            }

                            if (date_type != 0)
                            {
                                dates.Add(DateHelpers.ProcessDate(sd_oid, e, date_type, date_type_name));
                            }
                        }
                    }
                }
            }

            #endregion



            #region keywords

            // Chemicals list - do these first as Mesh list often duplicates them.

            XElement chemicals_list = citation.Element("ChemicalList");
            if (chemicals_list != null)
            {
                IEnumerable<XElement> chemicals = chemicals_list.Elements("Chemical");
                if (chemicals.Count() > 0)
                {
                    foreach (XElement ch in chemicals)
                    {
                        XElement chemName = ch.Element("NameOfSubstance");
                        ObjectTopic tp = new ObjectTopic
                        {
                            sd_oid = sd_oid,
                            topic = GetElementAsString(chemName),
                            ct_scheme = "MESH",
                            ct_scheme_id = 14,
                            topic_type_id = 12,
                            topic_type = "chemical / agent",
                            where_found = "chemicals list"
                        };

                        if (chemName != null)
                        {
                            tp.ct_scheme_code = GetAttributeAsString(chemName.Attribute("UI"));
                        }
                        topics.Add(tp);
                    }
                }
            }

            // Mesh headings list.

            XElement mesh_headings_list = citation.Element("MeshHeadingList");
            if (mesh_headings_list != null)
            {
                IEnumerable<XElement> mesh_headings = mesh_headings_list.Elements("MeshHeading");
                foreach (XElement e in mesh_headings)
                {
                    XElement desc = e.Element("DescriptorName");

                    // Create a simple mesh heading record.

                    ObjectTopic nt = new ObjectTopic
                    {
                        sd_oid = sd_oid,
                        topic = GetElementAsString(desc),
                        ct_scheme = "MESH",
                        ct_scheme_id = 14,
                        ct_scheme_code = GetAttributeAsString(desc.Attribute("UI")),
                        topic_type = (GetAttributeAsString(desc.Attribute("Type"))?.ToLower()),
                        where_found = "mesh headings"
                    };

                    // Check does not already exist (if it does, usually because it was in the chemicals list)

                    bool new_topic = true;
                    foreach (ObjectTopic t in topics)
                    {
                        if (t.topic.ToLower() == nt.topic.ToLower()
                            && (t.topic_type == nt.topic_type || (t.topic_type == "chemical / agent" && nt.topic_type == null)))
                        {
                            new_topic = false;
                            break;
                        }
                    }
                    if (new_topic) topics.Add(nt);


                    // if there are qualifiers, use these as the term type (or scope / context) 
                    // in further copies of the keyword
                    IEnumerable<XElement> qualifiers = e.Elements("QualifierName");
                    if (qualifiers.Count() > 0)
                    {
                        foreach (XElement em in qualifiers)
                        {
                            ObjectTopic qt = new ObjectTopic
                            {
                                sd_oid = sd_oid,
                                topic = nt.topic,
                                ct_scheme = "MESH",
                                ct_scheme_id = 14,
                                ct_scheme_code = nt.ct_scheme_code + "-" + GetAttributeAsString(em.Attribute("UI")),
                                topic_type = GetElementAsString(em),
                                where_found = "mesh headings"
                            };

                            topics.Add(qt);
                        }
                    }
                }
            }


            // Supplementary mesh list - rarely found.

            XElement suppmesh_list = citation.Element("SupplMeshList");
            if (suppmesh_list != null)
            {
                IEnumerable<XElement> supp_mesh_names = suppmesh_list.Elements("SupplMeshName");
                if (supp_mesh_names.Count() > 0)
                {
                    foreach (XElement s in supp_mesh_names)
                    {
                        ObjectTopic ts = new ObjectTopic
                        {
                            sd_oid = sd_oid,
                            topic = GetElementAsString(s),
                            ct_scheme = "MESH",
                            ct_scheme_id = 14,
                            ct_scheme_code = GetAttributeAsString(s.Attribute("UI")),
                            topic_type = (GetAttributeAsString(s.Attribute("Type"))?.ToLower()),
                            where_found = "supp mesh list"
                        };

                        topics.Add(ts);
                    }
                }
            }


            // Keywords.

            var keywords_lists = citation.Elements("KeywordList");
            if (keywords_lists.Count() > 0)
            {
                foreach (XElement e in keywords_lists)
                {
                    string this_owner = GetAttributeAsString(e.Attribute("Owner"));
                    IEnumerable<XElement> words = e.Elements("Keyword");
                    if (words.Count() > 0)
                    {
                        foreach (XElement k in words)
                        {
                            ObjectTopic kw = new ObjectTopic
                            {
                                sd_oid = c.sd_oid,
                                topic = GetElementAsString(k),
                                where_found = "keywords"
                            };

                            if (this_owner == "NOTNLM")
                            {
                                kw.ct_scheme = "Generated by authors";
                                kw.ct_scheme_id = 11;
                                kw.topic_type = "keyword";
                            }

                            topics.Add(kw);
                        }
                    }

                }
            }
            #endregion



            #region Identifiers

            // Article Elocations - can provide doi and publishers id.

            var locations = article.Elements("ELocationID");
            string source_elocation_string = "";
            if (locations.Count() > 0)
            {
                string valid_yn = null;
                string loctype = null;
                string value = null;
                foreach (XElement t in locations)
                {
                    valid_yn = GetAttributeAsString(t.Attribute("ValidYN"));
                    if (valid_yn.ToUpper() == "Y")
                    {
                        loctype = GetAttributeAsString(t.Attribute("EIdType"));
                        value = GetElementAsString(t);
                        if (loctype != null && value != null)
                        {
                            switch (loctype.ToLower())
                            {
                                case "pii":
                                    {
                                        ids.Add(new ObjectIdentifier(sd_oid, 34, "Publisher article ID", value, null, null));
                                        source_elocation_string += " pii:" + value + ".";
                                        break;
                                    }

                                case "doi":
                                    {
                                        if (c.doi == null) c.doi = value.Trim().ToLower();
                                        source_elocation_string += " doi:" + value + ".";
                                        break;
                                    }
                            }
                        }
                    }
                }
            }


            // Other ids.

            var other_ids = citation.Elements("OtherID");
            if (other_ids.Count() > 0)
            {
                string source = null;
                string other_id = null;
                foreach (XElement i in other_ids)
                {
                    source = GetAttributeAsString(i.Attribute("Source"));
                    other_id = GetElementAsString(i);
                    if (source != null && other_id != null)
                    {
                        // Both source and value are present, 
                        // only a few source types listed as possible.

                        if (source == "NLM")
                        {
                            if (other_id.Substring(0, 3) == "PMC")
                            {
                                ids.Add(new ObjectIdentifier(sd_oid, 31, "PMCID", other_id, 100133, "National Library of Medicine"));
                                ObjectInstance objinst = new ObjectInstance
                                {
                                    sd_oid = sd_oid,
                                    instance_type_id = 1,
                                    instance_type = "Full resource",
                                    url = "https://www.ncbi.nlm.nih.gov/pmc/articles/" + other_id.ToString(),
                                    url_accessible = true,
                                    repository_org_id = 100133,
                                    repository_org = "National Library of Medicine",
                                    resource_type_id = 36,
                                    resource_type = "Web text with download"
                                };
                                instances.Add(objinst);
                            }
                            else
                            {
                                ids.Add(new ObjectIdentifier(sd_oid, 32, "NIH Manuscript ID", other_id, 100134, "National Institutes of Health"));
                            }
                        }
                        else if (source == "NRCBL")
                        {
                            ids.Add(new ObjectIdentifier(sd_oid, 33, "NRCBL", other_id, 100447, "Georgetown University"));
                        }
                        else
                        {
                            string qText = "Unexpected source code (" + source + ") found in 'other IDs'";
                            repo.StoreExtractionNote(sd_oid, 21, qText);
                        }
                    }
                }
            }


            // Article id list. Can contain a variety of Ids, including (though rarely) a doi.
            // IdNotPresent is a helper function that checks that an id of a 
            // particular type has not already been extracted.

            XElement article_ids = pubmed.Element("ArticleIdList");
            if (article_ids != null)
            {
                IEnumerable<XElement> artids = article_ids.Elements("ArticleId");
                if (artids.Count() > 0)
                {
                    string id_type = null;
                    string other_id = null;
                    foreach (XElement artid in artids)
                    {
                        id_type = GetAttributeAsString(artid.Attribute("IdType"));
                        other_id = GetElementAsString(artid).Trim();
                        if (id_type != null && other_id != null)
                        {
                            switch (id_type.ToLower())
                            {
                                case "doi":
                                    {
                                        other_id = other_id.ToLower();
                                        if (c.doi == null)
                                        {
                                            c.doi = other_id;
                                        }
                                        else
                                        {
                                            if (c.doi != other_id)
                                            {
                                                string qText = "Two different dois have been supplied: " + c.doi + " from ELocation, and " + other_id + " from Article Ids";
                                                repo.StoreExtractionNote(sd_oid, 14, qText);
                                                break;
                                            }
                                        }
                                        break;
                                    }
                                case "pii":
                                    {
                                        if (IdentifierHelpers.IdNotPresent(ids, 34, other_id))
                                        {
                                            ids.Add(new ObjectIdentifier(sd_oid, 34, "Publisher article ID", other_id, null, null));
                                        }
                                        break;
                                    }

                                case "pmcpid": { ids.Add(new ObjectIdentifier(sd_oid, 37, "PMC Publisher ID", other_id, null, null)); break; }

                                case "pmpid": { ids.Add(new ObjectIdentifier(sd_oid, 38, "PM Publisher ID", other_id, null, null)); break; }

                                case "sici": { ids.Add(new ObjectIdentifier(sd_oid, 35, "Serial Item and Contribution Identifier ", other_id, null, null)); break; }

                                case "medline": { ids.Add(new ObjectIdentifier(sd_oid, 36, "Medline UID", other_id, 100133, "National Library of Medicine")); break; }

                                case "pubmed":
                                    {
                                        if (IdentifierHelpers.IdNotPresent(ids, 16, other_id))
                                        {
                                            // should be present already! - if a different value log it a a query
                                            string qText = "Two different values for pmid found: record pmiod is " + sd_oid + ", but in article ids the value " + other_id + " is listed";
                                            repo.StoreExtractionNote(sd_oid, 22, qText);
                                            ids.Add(new ObjectIdentifier(sd_oid, 16, "PMID", sd_oid, 100133, "National Library of Medicine"));
                                        }
                                        break;
                                    }
                                case "mid":
                                    {
                                        if (IdentifierHelpers.IdNotPresent(ids, 32, other_id))
                                        {
                                            ids.Add(new ObjectIdentifier(sd_oid, 32, "NIH Manuscript ID", other_id, 100134, "National Institutes of Health"));
                                        }
                                        break;
                                    }

                                case "pmc":
                                    {
                                        if (IdentifierHelpers.IdNotPresent(ids, 31, other_id))
                                        {
                                            ids.Add(new ObjectIdentifier(c.sd_oid, 31, "PMCID", other_id, 100133, "National Library of Medicine"));
                                            ObjectInstance objinst = new ObjectInstance
                                            {
                                                sd_oid = sd_oid,
                                                instance_type_id = 1,
                                                instance_type = "Full resource",
                                                url = "https://www.ncbi.nlm.nih.gov/pmc/articles/" + other_id.ToString(),
                                                url_accessible = true,
                                                repository_org_id = 100133,
                                                repository_org = "National Library of Medicine",
                                                resource_type_id = 36,
                                                resource_type = "Web text with download"
                                            };
                                            instances.Add(objinst);
                                        }
                                        break;
                                    }

                                case "pmcid":
                                    {
                                        if (IdentifierHelpers.IdNotPresent(ids, 31, other_id))
                                        {
                                            ids.Add(new ObjectIdentifier(c.sd_oid, 31, "PMCID", other_id, 100133, "National Library of Medicine"));
                                            ObjectInstance objinst = new ObjectInstance
                                            {
                                                sd_oid = sd_oid,
                                                instance_type_id = 1,
                                                instance_type = "Full resource",
                                                url = "https://www.ncbi.nlm.nih.gov/pmc/articles/" + other_id.ToString(),
                                                url_accessible = true,
                                                repository_org_id = 100133,
                                                repository_org = "National Library of Medicine",
                                                resource_type_id = 36,
                                                resource_type = "Web text with download"
                                            };
                                            instances.Add(objinst);
                                        }
                                        break;
                                    }
                                default:
                                    {
                                        string qText = "A unexpexted article id type (" + id_type + ") found a date in the article id section";
                                        repo.StoreExtractionNote(sd_oid, 23, qText);
                                        break;
                                    }
                            }
                        }
                    }
                }
            }


            // See if any article dates can be matched to the identifiers.

            foreach (ObjectIdentifier i in ids)
            {
                if (i.identifier_type_id == 16)
                {
                    // pmid
                    foreach (ObjectDate dt in dates)
                    {
                        if (dt.date_type_id == 62)
                        {
                            // date added to PubMed
                            i.date_applied = dt.date_as_string;
                            break;
                        }
                    }
                }

                // pmid date may be available as an entrez date
                if (i.identifier_type_id == 16 && i.date_applied == null)
                {
                    // pmid
                    foreach (ObjectDate dt in dates)
                    {
                        if (dt.date_type_id == 65)
                        {
                            // date added to Entrez (normally = date added to pubMed)
                            i.date_applied = dt.date_as_string;
                            break;
                        }
                    }
                }

                if (i.identifier_type_id == 31)
                {
                    // pmc id
                    foreach (ObjectDate dt in dates)
                    {
                        if (dt.date_type_id == 61)
                        {
                            // date added to PMC
                            i.date_applied = dt.date_as_string;
                            break;
                        }
                    }
                }

                if (i.identifier_type_id == 34)
                {
                    // publisher's id
                    foreach (ObjectDate dt in dates)
                    {
                        if (dt.date_type_id == 11)
                        {
                            // date of acceptance by publisher into their system
                            i.date_applied = dt.date_as_string;
                            break;
                        }

                    }
                }

                if (i.identifier_type_id == 36)
                {
                    // Medline UID
                    foreach (ObjectDate dt in dates)
                    {
                        if (dt.date_type_id == 63)
                        {
                            // date added to Medline
                            i.date_applied = dt.date_as_string;
                            break;
                        }

                    }
                }

            }

            #endregion



            #region People

            // Get author details. GetPersonalData is a helper function that
            // splits the author information up into its constituent classes.

            XElement author_list = article.Element("AuthorList");
            if (author_list != null)
            {
                var authors = author_list.Elements("Author");
                foreach (XElement a in authors)
                {
                    bool valid = GetAttributeAsBool(a.Attribute("ValidYN"));
                    if (valid)   // only use valid entries
                    {
                        // Construct the basic contributor data from the various elements.
                        string family_name = GetElementAsString(a.Element("LastName")) ?? "";
                        string given_name = GetElementAsString(a.Element("ForeName")) ?? "";
                        string suffix = GetElementAsString(a.Element("Suffix")) ?? "";
                        string initials = GetElementAsString(a.Element("Initials")) ?? "";
                        string collective_name = GetElementAsString(a.Element("CollectiveName")) ?? "";

                        if (given_name == "")
                        {
                            given_name = initials;
                        }

                        string full_name = "";
                        if (collective_name != "")
                        {
                            family_name = collective_name;
                            full_name = collective_name;
                            given_name = "";
                        }
                        else
                        {
                            full_name = ((given_name != "") ? given_name : initials + " " + family_name + suffix).Trim();
                        }

                        string identifier = "", identifier_source = "";
                        if (a.Elements("Identifier").Count() > 0)
                        {
                            var person_identifiers = a.Elements("Identifier");
                            foreach (XElement e in person_identifiers)
                            {
                                identifier = GetElementAsString(e).Trim();
                                identifier_source = GetAttributeAsString(e.Attribute("Source"));

                                // should only ever be a single ORCID identifier
                                if (identifier_source == "ORCID")
                                {
                                    identifier = StringHelpers.TidyORCIDIdentifier(identifier, sd_oid, repo);
                                    break;  // no need to look for more
                                }
                                else
                                {
                                    string qText = "person " + full_name + "(linked to " + sd_oid + ") identifier ";
                                    qText += "is not an ORCID (" + identifier + " (source =" + identifier_source + "))";
                                    repo.StoreExtractionNote(sd_oid, 27, qText);
                                    identifier = ""; identifier_source = "";  // do not store in db
                                }
                            }
                        }

                        string affiliation = "", affil_identifier = ""; string affil_ident_source = "";
                        if (a.Elements("AffiliationInfo").Count() > 0)
                        {
                            // N.B. ensure there is an identifier element or the attempted attribute access will throw an exception
                            // occasionally may be multiple affiliations
                            var person_affiliations = a.Elements("AffiliationInfo");
                            foreach (XElement e in person_affiliations)
                            {
                                affiliation = GetElementAsString(e.Element("Affiliation")) ?? "";
                                affil_identifier = GetElementAsString(e.Element("Identifier")) ?? "";
                                if (affil_identifier != "")
                                {
                                    affil_ident_source = GetAttributeAsString(e.Element("Identifier").Attribute("Source"));
                                    string qText = "person " + full_name + "(linked to " + sd_oid + ") affiliation given ";
                                    qText += "as Id and Id source  (" + affil_identifier + " (source =" + affil_ident_source + ")";
                                    repo.StoreExtractionNote(sd_oid, 25, qText);
                                }
                            }

                        }

                        contributors.Add(new ObjectContributor(sd_oid, 11, "Creator",
                                                         given_name, family_name, full_name,
                                                         identifier, identifier_source,
                                                         affiliation, affil_identifier, affil_ident_source));

                    }

                }

                // Construct author string for citation - exact form depends on numbers of ayuthors identified.

                if (contributors.Count == 1)
                {
                    author_string = contributors[0].person_family_name + " " + contributors[0].person_given_name?.Substring(0, 1).ToUpper();
                }

                else if (contributors.Count == 2)
                {
                    author_string = contributors[0].person_family_name + " " + contributors[0].person_given_name?.Substring(0, 1).ToUpper();
                    author_string += " & ";
                    author_string += contributors[1].person_family_name + " " + contributors[1].person_given_name?.Substring(0, 1).ToUpper();
                }

                else if (contributors.Count == 3)
                {
                    author_string = contributors[0].person_family_name + " " + contributors[0].person_given_name?.Substring(0, 1).ToUpper();
                    author_string += ", ";
                    author_string += contributors[1].person_family_name + " " + contributors[1].person_given_name?.Substring(0, 1).ToUpper();
                    author_string += " & ";
                    author_string += contributors[2].person_family_name + " " + contributors[2].person_given_name?.Substring(0, 1).ToUpper();

                }

                else if (contributors.Count > 3)
                {
                    author_string = contributors[0].person_family_name + " " + contributors[0].person_given_name?.Substring(0, 1).ToUpper();
                    author_string += ", ";
                    author_string += contributors[1].person_family_name + " " + contributors[1].person_given_name?.Substring(0, 1).ToUpper();
                    author_string += ", ";
                    author_string += contributors[2].person_family_name + " " + contributors[2].person_given_name?.Substring(0, 1).ToUpper();
                    author_string += " et al";
                }

                author_string = author_string.Trim();
            }

            #endregion


            #region Descriptions

            // Derive Journal source string (used as a descriptive element)...
            // Constructed as <MedlineTA>. Date;<Volume>(<Issue>):<Pagination>. <ELocationID>.
            // Needs to be extended to take into account the publication model and thus the other poossible dates
            // see https://www.nlm.nih.gov/bsd/licensee/elements_article_source.html

            if (JournalInfo != null)
            {
                string medline_ta = GetElementAsString(JournalInfo.Element("MedlineTA"));

                string date = (publication_date_string != null) ? publication_date_string : "";

                string volume = GetElementAsString(article.Element("Journal").Element("JournalIssue").Element("Volume"));
                if (volume == null) volume = "";

                string issue = GetElementAsString(article.Element("Journal").Element("JournalIssue").Element("Issue"));
                if (issue == null)
                {
                    issue = "";
                }
                else
                {
                    issue = "(" + issue + ")";
                }

                string pagination = "";
                XElement pagn = article.Element("Pagination");
                if (pagn != null)
                {
                    pagination = GetElementAsString(pagn.Element("MedlinePgn"));
                    if (pagination == null)
                    {
                        pagination = "";
                    }
                    else
                    {
                        pagination = ":" + pagination;
                    }
                }

                string public_date = "";
                string elec_date = "";
                if (pagination.EndsWith(";"))
                {
                    pagination = pagination.Substring(0, pagination.Length - 1);
                }

                switch (c.pub_model)
                {
                    case "Print":
                        {
                            // The date is taken from the PubDate element.

                            public_date = (publication_date_string != null) ? publication_date_string : "";
                            journal_source = medline_ta + ". " + public_date + ";" + volume + issue + pagination + ". " + source_elocation_string;
                            break;
                        }


                    case "Print-Electronic":
                        {
                            // The electronic date is before the print date but the publisher has selected the print date to be the date within the citation.
                            // The date in the citation therefore comes from the print publication date, PubDate.
                            // The electronic publishing date is then shown afterwards, as "Epub YYY MMM DD".

                            public_date = (publication_date_string != null) ? publication_date_string : "";
                            elec_date = (electronic_date_string != null) ? electronic_date_string : "";
                            journal_source = medline_ta + ". " + public_date + ";" + volume + issue + pagination + ". " + "Epub " + elec_date + "." + source_elocation_string;
                            break;
                        }

                    case "Electronic":
                        {
                            // Here there is no published hardcopy print version of the item. 
                            // If there is an ArticleDate element present in the citation it is used as the source of the publication date in the journal source string.
                            // If no ArticleDate element was provided the publication date is assumed to be that of an electronic publication.
                            // In either case there is no explicit indication that this is an electronic publication in the citation.

                            elec_date = (electronic_date_string != null) ? electronic_date_string : "";
                            if (elec_date == null)
                            {
                                elec_date = (publication_date_string != null) ? publication_date_string : "";
                            }
                            journal_source = medline_ta + ". " + elec_date + ";" + volume + issue + pagination + ". " + source_elocation_string;
                            break;
                        }

                    case "Electronic-Print":
                        {
                            // The electronic date is before the print date, but – in contrast to "Print - Electronic" – the publisher wishes the main citation date to be based on the electronic date (ArticleDate). 
                            // The source is followed by the print date notation using the content of the PubDate element.

                            public_date = (publication_date_string != null) ? publication_date_string : "";
                            elec_date = (electronic_date_string != null) ? electronic_date_string : "";
                            journal_source = medline_ta + ". " + elec_date + ";" + volume + issue + pagination + ". " + "Print " + public_date + "." + source_elocation_string;
                            break;
                        }

                    case "Electronic-eCollection":
                        {
                            // This is an electronic publication first, followed by inclusion in an electronic collection(similar to an issue).
                            // The publisher wants articles cited by the electronic article publication date.The citation therefore uses the ArticleDate as the source of the date, 
                            // but the eCollection date can be obtained from the PubDate element.

                            public_date = (publication_date_string != null) ? publication_date_string : "";
                            elec_date = (electronic_date_string != null) ? electronic_date_string : "";
                            journal_source = medline_ta + ". " + elec_date + ";" + volume + issue + pagination + ". " + "eCollection " + public_date + "." + source_elocation_string;
                            break;
                        }
                }

                // add the description
                descriptions.Add(new ObjectDescription
                {
                    sd_oid = c.sd_oid,
                    description_type_id = 18,
                    description_type = "Journal Source String",
                    description_text = journal_source,
                    lang_code = "eng"
                });
            }


             // Article abstracts.

             //XElement articleAbstract = article.Element("Abstract");
             //if (articleAbstract != null)
             //{
                //    bool ab_contains_html = false;
                //    IEnumerable<XElement> abstract_texts = articleAbstract.Elements("AbstractText");

                //    foreach (XElement at in abstract_texts)
                //    {
                //        var abreader = at.CreateReader();
                //        abreader.MoveToContent();
                //        string ab_text = abreader.ReadInnerXml().Trim();

                //        if (ab_text.Contains("<i>") || ab_text.Contains("<b>") || ab_text.Contains("<u>")
                //            || ab_text.Contains("<sup>") || ab_text.Contains("<sub>") || ab_text.Contains("<math>"))
                //        {
                //            ab_contains_html = true;
                //            // log this in extraction_notes
                //            string qText = "The abstract text includes embedded html (" + ab_text + ")";
                //            repo.StoreExtractionNote(pmid, 26, qText);
                //        }
                //        else
                //        {
                //            ab_contains_html = false;
                //        }

                //        Description abs = new Description
                //        {
                //            sd_id = c.sd_id,
                //            description_type_id = 16,
                //            description_type = "Abstract Section",
                //            label = GetAttributeAsString(at.Attribute("Label")),
                //            description_text = ab_text,
                //            lang_code = "eng",
                //            contains_html = ab_contains_html
                //        };

                //        descriptions.Add(abs);
                //    }
                //}


                //// Other abstracts (relatively rare).

                //IEnumerable<XElement> other_abstracts = citation.Elements("OtherAbstract");
                //if (other_abstracts.Count() > 0)
                //{
                //    foreach (XElement oab in other_abstracts)
                //    {
                //        bool ab_contains_html = false;
                //        IEnumerable<XElement> abstract_texts = oab.Elements("AbstractText");

                //        foreach (XElement at in abstract_texts)
                //        {
                //            var abreader = at.CreateReader();
                //            abreader.MoveToContent();
                //            string ab_text = abreader.ReadInnerXml().Trim();

                //            if (ab_text.Contains("<i>") || ab_text.Contains("<b>") || ab_text.Contains("<u>")
                //                || ab_text.Contains("<sup>") || ab_text.Contains("<sub>") || ab_text.Contains("<math>"))
                //            {
                //                ab_contains_html = true;
                //                // log this in extraction_notes
                //                string qText = "The abstract text includes embedded html (" + ab_text + ")";
                //                repo.StoreExtractionNote(pmid, 26, qText);
                //            }
                //            else
                //            {
                //                ab_contains_html = false;
                //            }


                //            Description abs = new Description
                //            {
                //                sd_id = c.sd_id,
                //                description_type_id = 17,
                //                description_type = "External Abstract",
                //                label = GetAttributeAsString(at.Attribute("Label")),
                //                description_text = ab_text,
                //                lang_code = "eng",
                //                contains_html = ab_contains_html
                //            };


                //            descriptions.Add(abs);
                //        }
                //    }
                //}

             #endregion


             #region Miscellaneous

             // Comment corrections list.

             //XElement comments_list = citation.Element("CommentsCorrectionsList");
             //if (comments_list != null)
             //{
             //    List<CommentCorrection> comments = comments_list
             //                .Elements("CommentsCorrections").Select(cc => new CommentCorrection
             //                {
             //                    sd_id = c.sd_id,
             //                    ref_type = GetAttributeAsString(cc.Attribute("RefType")),
             //                    ref_source = GetElementAsString(cc.Element("RefSource")),
             //                    pmid = GetElementAsString(cc.Element("PMID")),
             //                    pmid_version = (cc.Element("PMID") != null) ? GetAttributeAsString(cc.Element("PMID").Attribute("Version")) : null,
             //                    note = GetElementAsString(cc.Element("Note"))
             //                }).ToList();

             //    c.comments = comments;
             //}


            // Publication types.

             //XElement publication_type_list = article.Element("PublicationTypeList");
             //if (publication_type_list != null)
             //{
             //    string type_name;
             //    var pub_types = publication_type_list.Elements("PublicationType");
             //    if (pub_types.Count() > 0)
             //    {
             //        foreach (var pub in pub_types)
             //        {
             //            type_name = GetElementAsString(pub);
             //            if (type_name == "Review" || type_name == "Case Reports" || type_name == "Meta - Analysis" ||
             //                type_name == "Video - Audio Media" || type_name == "Systematic Review" || type_name == "English Abstract" ||
             //                type_name == "Retracted Publication" || type_name == "Webcasts")
             //            {
             //                pubtypes.Add(new Publication_Type(c.sd_id, type_name));
             //            }
             //        }
             //    }
             //}

             #endregion

             // Tidy up article title and then the display title.

             if (!art_title.EndsWith(".") && !art_title.EndsWith("?") && !art_title.EndsWith(";"))
            {
                art_title = art_title + ".";
            }
             c.display_title = (author_string != "" ? author_string + ". " : "") + art_title + journal_source;

             // Assign repeating properties to citationn object
             // and return the dully constructed citation object.
 
             c.object_dates = dates;
             c.object_titles = titles;
             c.object_identifiers = ids;
             c.object_languages = object_languages;
             c.object_contributors = contributors;
             c.object_descriptions = descriptions;
             c.publication_types = pubtypes;
             c.object_instances = instances;
             c.object_topics = topics;

             // constant for now
             c.datetime_of_data_fetch = new DateTime(2020, 5, 17);


             return c;
        }


        public void StoreData(DataLayer repo, CitationObject c)
        {
            // A routine called by the main program that runs through the 
            // Citation object - the singleton properties followed by each of the 
            // repeating propereties, and stores them in the associated DB table.

            // Create the base Citation record and store it.

            CitationObject_in_DB cdb = new CitationObject_in_DB
            {
                sd_oid = c.sd_oid,
                sd_id_version = c.sd_oid_version,
                display_title = c.display_title,
                doi = c.doi,
                status = c.status,
                pub_model = c.pub_model,
                publication_year = c.publication_year,
                publication_status = c.publication_status,
                journal_title = c.journal_title,
                pissn = c.pissn,
                eissn = c.eissn,
                datetime_of_data_fetch = c.datetime_of_data_fetch
            };

            repo.StoreDataObject(cdb);

            // Store the contributors, their identifiers and attributions. 

            if (c.object_instances.Count > 0)
            {
                repo.StoreObjectInstances(ObjectCopyHelpers.object_instances_helper,
                                            c.object_instances);
            }

            if (c.object_titles.Count > 0)
            {
                repo.StoreObjectTitles(ObjectCopyHelpers.object_titles_helper,
                                            c.object_titles);
            }

            if (c.object_dates.Count > 0)
            {
                repo.StoreObjectDates(ObjectCopyHelpers.object_dates_helper,
                                            c.object_dates);
            }

            if (c.object_contributors != null) repo.StoreObjectContributors(ObjectCopyHelpers.object_contributor_copyhelper, c.object_contributors);

            //if (c.contrib_identifiers != null) repo.StoreContribIdentifiers(h.persid_copyhelper, c.contrib_identifiers);

            //if (c.contrib_affiliations != null) repo.StoreContribAffiliations(h.persaff_copyhelper, c.contrib_affiliations);

            // Store the object id, date, language and description records.

            if (c.object_identifiers != null) repo.StoreObjectIdentifiers(ObjectCopyHelpers.object_identifier_copyhelper, c.object_identifiers);

            if (c.object_languages != null) repo.StoreObjectLanguages(ObjectCopyHelpers.object_language_copyhelper, c.object_languages);

            if (c.object_descriptions != null) repo.StoreObjectDescriptions(ObjectCopyHelpers.object_description_copyhelper, c.object_descriptions);

            // Store the object instance, accession number, publication type and title records, and any comments.

            if (c.accession_numbers != null) repo.StoreObjectAcessionNumbers(ObjectCopyHelpers.object_accession_number_copyhelper, c.accession_numbers);

            //if (c.publication_types != null) r.StorePublicationTypes(h.pub_type_copyhelper, c.publication_types);

            if (c.comments != null) repo.StoreObjectComments(ObjectCopyHelpers.object_correction_copyhelper, c.comments);

            if (c.object_topics != null) repo.StoreObjectTopics(ObjectCopyHelpers.object_topic_copyhelper, c.object_topics);

        }


        #region Helper functions

        // These functions make use of the explicit cast operators
        // available for XElement and XAttribute.
        // Most functions include a preliminary check for the existence
        // of the Element or Attribute node itself, followed by a cast to
        // the required type of the Element or Attribute's 
        // Value (= inner HTML for an element).

        public string GetElementAsString(XElement e) => (e == null) ? null : (string)e;

        public string GetAttributeAsString(XAttribute a) => (a == null) ? null : (string)a;

        public int? GetElementAsInt(XElement e) => (e == null) ? null : (int?)e;

        public int? GetAttributeAsInt(XAttribute a) => (a == null) ? null : (int?)a;

        public bool GetAttributeAsBool(XAttribute a)
        {
            string avalue = GetAttributeAsString(a);
            if (avalue != null)
            {
                return (avalue.ToUpper() == "Y") ? true : false;
            }
            else
            {
                return false;
            }
        }
       
        #endregion
    }
}



