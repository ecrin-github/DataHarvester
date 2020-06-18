using System;
using System.Collections.Generic;
using Dapper;
using Dapper.Contrib;
using System.Text;
using Dapper.Contrib.Extensions;

namespace DataHarvester.pubmed
{
    // Full Data Object model, constructed during processing
    // as a representation of the full PubMed Data object
    /*
    public class DataObject
    {
        public int sd_id { get; set; }
        public int? sd_id_version { get; set; }
        public string display_title { get; set; }
        public string doi { get; set; }
        public string status { get; set; }
        public string pub_model { get; set; }
        public int? publication_year { get; set; }
        public string publication_status { get; set; }
        public string journal_title { get; set; }
        public string pissn { get; set; }
        public string eissn { get; set; }
        public DateTime? datetime_of_data_fetch { get; set; }

        public List<string> languages { get; set; }
        public List<ObjectTitle> object_titles { get; set; }
        public List<Description> object_descriptions { get; set; }
        public List<ObjectLanguage> object_languages { get; set; }
        public List<Contributor> object_contributors { get; set; }
        public List<Person_Identifier> contrib_identifiers { get; set; }
        public List<Person_Affiliation> contrib_affiliations { get; set; }
        public List<Instance> object_instances { get; set; }
        public List<DB_Accession_Number> accession_numbers { get; set; }
        public List<Topic> object_topics { get; set; }
        public List<Identifier> object_identifiers { get; set; }
        public List<ObjectDate> object_dates { get; set; }
        public List<Publication_Type> publication_types { get; set; }
        public List<CommentCorrection> comments { get; set; }

    }


    // Contributor class, a Data Object component

    public class Contributor
    {
        public int sd_id { get; set; }
        public int person_id { get; set; }
        public int contributor_type_id { get; set; }
        public string contributor_type { get; set; }
        public string family_name { get; set; }
        public string given_name { get; set; }
        public string suffix { get; set; }
        public string initials { get; set; }
        public string collective_name { get; set; }

    }

    // Person Identifier class, a Data Object component,
    // Stored as part of the contributor record, linked by the person id

    public class Person_Identifier
    {
        public int sd_id { get; set; }
        public int person_id { get; set; }
        public string identifier { get; set; }
        public string identifier_source { get; set; }
    }

    // Person affiliation class, a Data Object component,
    // Stored as part of the contributor record, linked by the person id

    public class Person_Affiliation
    {
        public int sd_id { get; set; }
        public int person_id { get; set; }
        public string affiliation { get; set; }
        public string affil_identifier { get; set; }
        public string affil_ident_source { get; set; }
    }

    // ObjectDate class, a Data Object component
    
    public class ObjectDate
    {
        public int sd_id { get; set; }
        public int date_type_id { get; set; }
        public string date_type { get; set; }
        public string date_as_string { get; set; }
        public bool? date_is_range { get; set; }
        public bool? date_is_partial { get; set; }
        public int? start_year { get; set; }
        public int? start_month { get; set; }
        public int? start_day { get; set; }
        public int? end_year { get; set; }
        public int? end_month { get; set; }
        public int? end_day { get; set; }

        public ObjectDate(int _sd_id, int _date_type_id, string _date_type, string _date_as_string, int? _year)
        {
            sd_id = _sd_id;
            date_type_id = _date_type_id;
            date_type = _date_type;
            date_as_string = _date_as_string;
            start_year = _year;
        }

        public ObjectDate(int _sd_id, int _date_type_id, string _date_type, int? _year, int? _month, int? _day)
        {
            sd_id = _sd_id;
            date_type_id = _date_type_id;
            date_type = _date_type;
            start_year = _year;
            start_month = _month;
            start_day = _day;
        }
    }

    // ObjectTitle class, a Data Object component

    public class ObjectTitle
    {
        public int sd_id { get; set; }
        public int title_type_id { get; set; }
        public string title_type { get; set; }
        public string title_text { get; set; }
        public bool? is_default { get; set; }
        public string lang_code { get; set; }
        public int? lang_status_id { get; set; }
        public bool? contains_html { get; set; }
        public string comments { get; set; }

        public ObjectTitle(int _sd_id, int _type_id, string _type_name, string _title_text,
                            bool _is_default, string _lang_code, int _lang_status_id,
                            bool _contains_html, string _comments)
        {
            sd_id = _sd_id; 
            title_type_id = _type_id;
            title_type = _type_name;
            title_text = _title_text;
            is_default = _is_default;
            lang_code = _lang_code;
            lang_status_id = _lang_status_id;
            contains_html = _contains_html;
            comments = _comments;
        }

    }


    // (Object) Identifier class, a Data Object component

    public class Identifier
    {
        public int sd_id { get; set; }
        public int identifier_type_id { get; set; }
        public string identifier_type { get; set; }
        public string identifier_value { get; set; }
        public int? identifier_org_id { get; set; }
        public string identifier_org { get; set; }
        public string date_applied { get; set; }

        public Identifier(int _sd_id, int _type_id, string _type_name,
                string _id_value, int? _org_id, string _org_name)
        {
            sd_id = _sd_id; 
            identifier_type_id = _type_id;
            identifier_type = _type_name;
            identifier_value = _id_value;
            identifier_org_id = _org_id;
            identifier_org = _org_name;
        }
    }

    // (Object) Instance class, a Data Object component

    public class Instance
    {
        public int sd_id { get; set; }
        public int? repository_org_id { get; set; }
        public string repository_org { get; set; }
        public int instance_type_id { get; set; }
        public string instance_type { get; set; }
        public string url { get; set; }
        public bool? url_direct_access { get; set; }
        public DateTime? url_last_checked { get; set; }
        public int? resource_type_id { get; set; }
        public string resource_type { get; set; }

    }


    // (Object) Description class, a Data Object component

    public class Description
    {
        public int sd_id { get; set; }
        public int description_type_id { get; set; }
        public string description_type { get; set; }
        public string label { get; set; }
        public string description_text { get; set; }
        public string lang_code { get; set; }
        public bool? contains_html { get; set; }
    }


    // (Object) Database Accession Number class, a Data Object component

    public class DB_Accession_Number
    {
        public int sd_id { get; set; }
        public int bank_id { get; set; }
        public string bank_name { get; set; }
        public string accession_number { get; set; }
    }


    // (Object) Publication Type class, a Data Object component

    public class DataObjectPublication_Type
    {
        public int sd_id { get; set; }
        public string type_name { get; set; }
        public DataObjectPublication_Type(int _sd_id, string _type_name)
        {
            sd_id = _sd_id;
            type_name = _type_name;
        }
    }


    // (Object) Comment Correction class, a Data Object component

    public class DataObjectCommentCorrection
    {
        public int sd_id { get; set; }
        public string ref_type { get; set; }
        public string ref_source { get; set; }
        public string pmid { get; set; }
        public string pmid_version { get; set; }
        public string note { get; set; }
    }


    // (Object) topic, a Data Object component.
    // For some reason the usual helper mechanism gives an error
    // so Dapper is used to store the class directly (in a loop).

    [Table("sd.object_topics")]
    public class Topic
    {
        public int sd_id { get; set; }
        public string topic { get; set; }
        public int topic_type_id { get; set; }
        public string topic_type { get; set; }
        public int? ct_scheme_id { get; set; }
        public string ct_scheme { get; set; }
        public string ct_scheme_code { get; set; }
        public string where_found { get; set; }

    }


    // The Object language class, essentkially just
    // a string language code attached to the source data Id

    public class ObjectLanguage
    {
        public int sd_id { get; set; }
        public string lang_code { get; set; }
        public ObjectLanguage(int _sd_id, string _lang_code)
        {
            sd_id = _sd_id;
            lang_code = _lang_code;
        }
    }

    // The class used to store data in the Data_objects table -
    // essentially the Data Object without its repeating components.

    [Table("sd.Data_objects")]
    public class Data_in_DB
    {
        [ExplicitKey]
        public int sd_id { get; set; }
        public int? sd_id_version { get; set; }
        public string display_title { get; set; }
        public string doi { get; set; }
        public string status { get; set; }
        public string pub_model { get; set; }
        public int? publication_year { get; set; }
        public string publication_status { get; set; }
        public string journal_title { get; set; }
        public string pissn { get; set; }
        public string eissn { get; set; }
        public DateTime? datetime_of_data_fetch { get; set; }
    }


    // Used during the extraction process, to log any odd or error condition.
    // The data are transfered directly to the database as required using Dapper.

    [Table("pp.extraction_notes")]
    public class ExtractionNote
    {
        public int id { get; set; }
        public int? sd_id { get; set; }
        public int note_type { get; set; }
        public string note { get; set; }
        public ExtractionNote(int? _sd_id, int _note_type, string _note)
        {
            sd_id = _sd_id;
            note_type = _note_type;
            note = _note;
        }
    }


    public class FileEntry
    {   
        public int id { get; set; }
        public int sd_id { get; set; }
        public string local_path { get; set; }
    }

    */

}

