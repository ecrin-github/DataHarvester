using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Serialization;

namespace DataHarvester.isrctn
{
    /*
    public class FileRecord
    {
        public int source_id { get; set; }
        public string sd_id { get; set; }
        public string remote_url { get; set; }
        public int lastsf_id { get; set; }
        public DateTime? remote_last_revised { get; set; }
        public int download_status { get; set; }
        public DateTime? download_datetime { get; set; }
        public string local_path { get; set; }
        public DateTime? local_last_revised { get; set; }

        public FileRecord(string _sd_id, string _remote_url, DateTime? _remote_last_revised, string _local_path)
        {
            source_id = 100126;
            sd_id = _sd_id;
            remote_url = _remote_url;
            lastsf_id = 100003;
            remote_last_revised = _remote_last_revised;
            download_status = 2;
            download_datetime = DateTime.Now;
            local_path = _local_path;
            local_last_revised = DateTime.Now;
        }
    }
    */


    class StudySearchEntry
    {
        public int Id { get; set; }
        public string ISRCTNNumber { get; set; }
        public string StudyName { get; set; }
        public string OverallStatus { get; set; }
        public string RecruitmentStatus { get; set; }
        public string DateAssigned { get; set; }
    }

    public class ISCTRN_Record
    {
        public int id { get; set; }
        public string isctrn_id { get; set; }
        public string doi { get; set; }
        public string study_name { get; set; }
        public string condition_category { get; set; }
        public DateTime? date_assigned { get; set; }
        public DateTime? last_edited { get; set; }
        public string registration_type { get; set; }
        public string trial_status { get; set; }
        public string recruitment_status { get; set; }
        public string background { get; set; }
        public string trial_website { get; set; }
        public string website_link { get; set; }

        public List<Item> contacts { get; set; }
        public List<Item> identifiers { get; set; }
        public List<Item> study_info { get; set; }
        public List<Item> eligibility { get; set; }
        public List<Item> locations { get; set; }
        public List<Item> sponsor { get; set; }
        public List<Item> funders { get; set; }
        public List<Item> publications { get; set; }
        public List<Item> additional_files { get; set; }
        public List<Item> notes { get; set; }
    }

    public class Item : IComparer<Item>
    {
        public int seq_id { get; set; }
        public string item_name { get; set; }
        public string item_value { get; set; }

        public Item(int _seq_id, string _item_name, string _item_value)
        {
            seq_id = _seq_id;
            item_name = _item_name;
            item_value = _item_value;
        }

        public Item()
        { }

        int IComparer<Item>.Compare(Item a, Item b)
        {
            if (a.seq_id > b.seq_id)
                return 1;
            if (a.seq_id < b.seq_id)
                return -1;
            else
                return 0;
        }
    }

}
