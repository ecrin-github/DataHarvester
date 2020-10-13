using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;

namespace DataHarvester
{
	[Table("sf.source_parameters")]
	public class Source
	{
		public int id { get; set; }
		public int? preference_rating { get; set; }
		public string database_name { get; set; }
		public int default_harvest_type_id { get; set; }
		public bool requires_file_name { get; set; }
		public bool uses_who_harvest { get; set; }
		public string local_folder { get; set; }
		public bool? local_files_grouped { get; set; }
		public int? grouping_range_by_id { get; set; }
		public string local_file_prefix { get; set; }
		public bool has_study_tables { get; set; }
		public bool has_study_topics { get; set; }
		public bool has_study_features { get; set; }
		public bool has_study_contributors { get; set; }
		public bool has_study_references { get; set; }
		public bool has_study_relationships { get; set; }
		public bool has_study_links { get; set; }
		public bool has_study_ipd_available { get; set; }
		public bool has_object_datasets { get; set; }
		public bool has_object_dates { get; set; }
		public bool has_object_rights { get; set; }
		public bool has_object_relationships { get; set; }
		public bool has_object_pubmed_set { get; set; }
	}

	[Table("sf.extraction_notes")]
	public class ExtractionNote
	{
		public int id { get; set; }
		public int source_id { get; set; }
		public string sd_id { get; set; }
		public string event_type { get; set; }
		public int event_type_id { get; set; }
		public int? note_type_id { get; set; }
		public string note { get; set; }

		public ExtractionNote(int _source_id, string _sd_id, string _event_type,
			                  int _event_type_id, int? _note_type_id, string _note)
		{
			source_id = _source_id;
			sd_id = _sd_id;
			event_type = _event_type;
			event_type_id = _event_type_id;
			note_type_id = _note_type_id;
			note = _note;
		}
	}


	[Table("sf.harvest_events")]
	public class HarvestEvent
	{
		[ExplicitKey]
		public int id { get; set; }
		public int source_id { get; set; }
		public int type_id { get; set; }
		public DateTime? time_started { get; set; }
		public DateTime? time_ended { get; set; }
		public int? num_records_available { get; set; }
		public int? num_records_harvested { get; set; }
		public string comments { get; set; }

		public HarvestEvent(int _id, int _source_id, int _type_id)
		{
			id = _id;
			source_id = _source_id;
			type_id = _type_id;
			time_started = DateTime.Now;
		}

		public HarvestEvent() { }
	}

	[Table("sf.source_data_studies")]
	public class StudyFileRecord
	{
		public int id { get; set; }
		public int source_id { get; set; }
		public string sd_id { get; set; }
		public string remote_url { get; set; }
		public DateTime? last_revised { get; set; }
		public bool? assume_complete { get; set; }
		public int download_status { get; set; }
		public string local_path { get; set; }
		public int last_saf_id { get; set; }
		public DateTime? last_downloaded { get; set; }
		public int last_harvest_id { get; set; }
		public DateTime? last_harvested { get; set; }
		public int last_import_id { get; set; }
		public DateTime? last_imported { get; set; }

		// constructor when a revision data can be expected (not always there)
		public StudyFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
											  DateTime? _last_revised, string _local_path)
		{
			source_id = _source_id;
			sd_id = _sd_id;
			remote_url = _remote_url;
			last_saf_id = _last_saf_id;
			last_revised = _last_revised;
			download_status = 2;
			last_downloaded = DateTime.Now;
			local_path = _local_path;
		}

		// constructor when an 'assumed complete' judgement can be expected (not always there)
		public StudyFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
											  bool? _assume_complete, string _local_path)
		{
			source_id = _source_id;
			sd_id = _sd_id;
			remote_url = _remote_url;
			last_saf_id = _last_saf_id;
			assume_complete = _assume_complete;
			download_status = 2;
			last_downloaded = DateTime.Now;
			local_path = _local_path;
		}


		public StudyFileRecord()
		{ }

	}


	[Table("sf.source_data_objects")]
	public class ObjectFileRecord
	{
		public int id { get; set; }
		public int source_id { get; set; }
		public string sd_id { get; set; }
		public string remote_url { get; set; }
		public DateTime? last_revised { get; set; }
		public bool? assume_complete { get; set; }
		public int download_status { get; set; }
		public string local_path { get; set; }
		public int last_saf_id { get; set; }
		public DateTime? last_downloaded { get; set; }
		public int last_harvest_id { get; set; }
		public DateTime? last_harvested { get; set; }
		public int last_import_id { get; set; }
		public DateTime? last_imported { get; set; }

		// constructor when a revision data can be expected (not always there)
		public ObjectFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
											  DateTime? _last_revised, string _local_path)
		{
			source_id = _source_id;
			sd_id = _sd_id;
			remote_url = _remote_url;
			last_saf_id = _last_saf_id;
			last_revised = _last_revised;
			download_status = 2;
			last_downloaded = DateTime.Now;
			local_path = _local_path;
		}

		// constructor when an 'assumed complete' judgement can be expected (not always there)
		public ObjectFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
											  bool? _assume_complete, string _local_path)
		{
			source_id = _source_id;
			sd_id = _sd_id;
			remote_url = _remote_url;
			last_saf_id = _last_saf_id;
			assume_complete = _assume_complete;
			download_status = 2;
			last_downloaded = DateTime.Now;
			local_path = _local_path;
		}

		public ObjectFileRecord()
		{ }

	}
}
