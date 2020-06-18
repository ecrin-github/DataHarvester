using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Serialization;

namespace DataHarvester.euctr
{
    /*
    public class FileRecord
    {
        public int source_id { get; set; }
        public string sd_id { get; set; }
        public string remote_url { get; set; }
        public int remote_lastsf_id { get; set; }
        public DateTime? remote_last_revised { get; set; }
        public int download_status { get; set; }
        public DateTime? download_datetime { get; set; }
        public string local_path { get; set; }
        public DateTime? local_last_revised { get; set; }

        public FileRecord(string _sd_id, string _remote_url, DateTime? _remote_last_revised, string _local_path)
        {
            source_id = 100123;
            sd_id = _sd_id;
            remote_url = _remote_url;
            remote_lastsf_id = 100002;
            remote_last_revised = _remote_last_revised;
            download_status = 2;
            download_datetime = DateTime.Now;
            local_path = _local_path;
            local_last_revised = DateTime.Now;
        }
    }

    */

    public class EUCTR_Record
    {
        public int id { get; set; }
        public string eudract_id { get; set; }
        public string sponsor_id { get; set; }
        public string sponsor_name { get; set; }
        public string start_date { get; set; }
        public string competent_authority { get; set; }
        public string trial_type { get; set; }
        public string trial_status { get; set; }
        public string medical_condition { get; set; }
        public string population_age { get; set; }
        public string gender { get; set; }
        public string details_url { get; set; }
        public string results_url { get; set; }
        public string entered_in_db { get; set; }

        public List<MeddraTerm> meddra_terms { get; set; }

        public List<DetailLine> identifiers { get; set; }
        public List<DetailLine> sponsors { get; set; }
        public List<ImpLine> imps { get; set; }
        public List<DetailLine> features { get; set; }
        public List<DetailLine> population { get; set; }

        public EUCTR_Record()
        {

        }
    }


    public class MeddraTerm
    {
        public string version { get; set; }
        public string soc_term { get; set; }
        public string code { get; set; }
        public string term { get; set; }
        public string level { get; set; }
    }


    public class DetailLine
    {
        public string item_code { get; set; }
        public string item_name { get; set; }
        public int item_number { get; set; }

        [XmlArray("values")]
        [XmlArrayItem("value")]
        public List<item_value> item_values { get; set; }
    }

    public class ImpLine
    {
        public int imp_number { get; set; }
        public string item_code { get; set; }
        public string item_name { get; set; }
        public int item_number { get; set; }

        [XmlArray("values")]
        [XmlArrayItem("value")]
        public List<item_value> item_values { get; set; }
    }

    public class item_value
    {
        [XmlText]
        public string value { get; set; }

        public item_value(string _value)
        {
            value = _value;
        }

        public item_value()
        { }
    }


    public class file_record
    {
        public int id { get; set; }
        public string local_path { get; set; }

    }

}
