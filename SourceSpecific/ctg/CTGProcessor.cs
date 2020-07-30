using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DataHarvester.ctg
{
	public class CTGProcessor
	{

		public Study ProcessData(FullStudy fs, DateTime? download_datetime, DataLayer common_repo)
		{
			Study s = new Study();
			List<StudyIdentifier> identifiers = new List<StudyIdentifier>();
			List<StudyTitle> titles = new List<StudyTitle>();
			List<StudyContributor> contributors = new List<StudyContributor>();
			List<StudyReference> references = new List<StudyReference>();
			List<StudyLink> studylinks = new List<StudyLink>();
			List<AvailableIPD> ipd_info = new List<AvailableIPD>();
			List<StudyTopic> topics = new List<StudyTopic>();
			List<StudyFeature> features = new List<StudyFeature>();
			List<StudyRelationship> relationships = new List<StudyRelationship>();

			List<DataObject> data_objects = new List<DataObject>();
			List<ObjectTitle> object_titles = new List<ObjectTitle>();
			List<ObjectDate> object_dates = new List<ObjectDate>();
			List<ObjectInstance> object_instances = new List<ObjectInstance>();

			string sid = null;
			string submissionDate = null;
			string official_title = null;
			string acronym = null;
			string brief_title = null;
			string status_verified_date = null;
			bool results_data_present = false;
			string sponsor_name = null;
			SplitDate firstpost = null, resultspost = null, updatepost = null, startdate = null;

			StructType IdentificationModule = null;
			StructType StatusModule = null;
			StructType SponsorCollaboratorsModule = null;
			StructType DescriptionModule = null;
			StructType ConditionsModule = null;
			StructType DesignModule = null;
			StructType EligibilityModule = null;
			StructType ContactsLocationsModule = null;
			StructType ReferencesModule = null;
			StructType IPDSharingStatementModule = null;
			StructType LargeDocumentModule = null;
			StructType ConditionBrowseModule = null;
			StructType InterventionBrowseModule = null;
		
			var Sections = Array.ConvertAll(fs.Struct.Items, item => (StructType)item);

			foreach (StructType st in Sections)
			{
				var Modules = Array.ConvertAll(st.Items, item => (StructType)item);
				switch (st.Name)
				{
				case "ProtocolSection":
					{
						// useful to get the NCT ID and the submission date before other data
						// as they are used 'out of sequence' in the sections that follow
						IdentificationModule = FindModule(Modules, "IdentificationModule");
						StatusModule = FindModule(Modules, "StatusModule");
						SponsorCollaboratorsModule = FindModule(Modules, "SponsorCollaboratorsModule");
						DescriptionModule = FindModule(Modules, "DescriptionModule");
						ConditionsModule = FindModule(Modules, "ConditionsModule");
						DesignModule = FindModule(Modules, "DesignModule");
						EligibilityModule = FindModule(Modules, "EligibilityModule");
						ContactsLocationsModule = FindModule(Modules, "ContactsLocationsModule");
						ReferencesModule = FindModule(Modules, "ReferencesModule");
						IPDSharingStatementModule = FindModule(Modules, "IPDSharingStatementModule");
						break;
					}

				case "ResultsSection":
					{
						bool ParticipantFlowModuleExists = CheckModuleExists(Modules, "ParticipantFlowModule");
						bool BaselineCharacteristicsModuleExists = CheckModuleExists(Modules, "BaselineCharacteristicsModule");
						bool OutcomeMeasuresModuleExists = CheckModuleExists(Modules, "OutcomeMeasuresModules");
						results_data_present = (ParticipantFlowModuleExists || BaselineCharacteristicsModuleExists
							|| OutcomeMeasuresModuleExists);
						break;
					}

				case "DocumentSection":
					{
						LargeDocumentModule = FindModule(Modules, "LargeDocumentModule");
						break;
					}

				case "DerivedSection":
					{
						ConditionBrowseModule = FindModule(Modules, "ConditionBrowseModule");
						InterventionBrowseModule = FindModule(Modules, "InterventionBrowseModule");
						break;
					}
				}
			}


			// these two modules considered together, as both are fundamental,
			// and related study data structures require data from both modules
			if (IdentificationModule != null && StatusModule != null)
			{
				object[] id_items = IdentificationModule.Items;
				object[] status_items = StatusModule.Items;

				// The NCT ID - extract as function wide variable first  
				sid = FieldValue(id_items, "NCTId");
				s.sd_sid = sid;
				s.datetime_of_data_fetch = download_datetime;

				s.study_status = FieldValue(status_items, "OverallStatus");
				s.study_status_id = TypeHelpers.GetStatusId(s.study_status);
				status_verified_date = FieldValue(status_items, "StatusVerifiedDate");

				submissionDate = FieldValue(status_items, "StudyFirstSubmitDate");

				// add the NCT identifier record - 100120 is the id of ClinicalTrials.gov
				submissionDate = DateHelpers.StandardiseDateFormat(submissionDate);
				identifiers.Add(new StudyIdentifier(sid, sid, 11, "Trial Registry ID", 100120,
													"ClinicalTrials.gov", submissionDate, null));

				// add title records
				bool default_found = false, is_default = false;
				brief_title = FieldValue(id_items, "BriefTitle");
				if (brief_title != null)
				{
					is_default = true;
					default_found = true;
					titles.Add(new StudyTitle(sid, brief_title, 15, "Public Title", is_default));
				}

				official_title = FieldValue(id_items, "OfficialTitle");
				if (official_title != null)
				{
					is_default = !default_found;
					default_found = true;
					titles.Add(new StudyTitle(sid, official_title, 17, "Protocol Title", is_default));
				}

				acronym = FieldValue(id_items, "Acronym");
				if (acronym != null)
				{
					is_default = !default_found;
					default_found = true;
					titles.Add(new StudyTitle(sid, acronym, 14, "Acronym or Abbreviation", is_default));
				}

				// select the appropriate display title
				s.display_title = (brief_title != null) ? brief_title : official_title;

				// get the sponsor id information
				string org = StringHelpers.TidyOrgName(StructFieldValue(id_items, "Organization", "OrgFullName"), sid);
				string org_study_id = StructFieldValue(id_items, "OrgStudyIdInfo", "OrgStudyId");
				string org_id_type = StructFieldValue(id_items, "OrgStudyIdInfo", "OrgStudyIdType");
				string org_id_domain = StringHelpers.TidyOrgName(StructFieldValue(id_items, "OrgStudyIdInfo", "OrgStudyIdDomain"), sid);
				string org_id_link = StructFieldValue(id_items, "OrgStudyIdInfo", "OrgStudyIdLink");

				// add the sponsor's identifier
				if (org_id_type == "U.S. NIH Grant/Contract")
				{
					identifiers.Add(new StudyIdentifier(sid, org_study_id,
											13, "Funder’s ID", 100134, "National Institutes of Health",
											null, org_id_link));
				}
				else
				{
					if (org == "[Redacted]")
					{
						org = "(sponsor name redacted in registry record)";
						identifiers.Add(new StudyIdentifier(sid, org_study_id,
								14, "Sponsor’s ID", 13, org, null, null));
					}
					else
					{
						identifiers.Add(new StudyIdentifier(sid, org_study_id,
								14, "Sponsor’s ID", null, org_id_domain ?? org,
								null, org_id_link));
					}
				}


				// add any additional identifiers (if not already used as a sponsor id)
				StructType[] secIds = ListStructs(id_items, "SecondaryIdInfoList");
				if (secIds != null)
				{
					foreach (StructType id in secIds)
					{
						var sec_items = id.Items;
						if (sec_items != null)
						{
							string id_value = FieldValue(sec_items, "SecondaryId");
							string id_link = FieldValue(sec_items, "SecondaryIdLink");
							if (org_study_id == null || id_value.Trim().ToLower() != org_study_id.Trim().ToLower())
							{
								string identifier_type = FieldValue(sec_items, "SecondaryIdType");
								string identifier_org = FieldValue(sec_items, "SecondaryIdDomain");
								IdentifierDetails idd = IdentifierHelpers.GetIdentifierProps(identifier_type, identifier_org, id_value);

								// add the secondary identifier
								identifiers.Add(new StudyIdentifier(sid, idd.id_value, idd.id_type_id, idd.id_type,
																idd.id_org_id, idd.id_org, null, id_link));
							}
						}
					}
				}

				// get the main three registry entry dates if they are available
				StructType FirstPostDate = FindStruct(status_items, "StudyFirstPostDateStruct");
				if (FirstPostDate != null)
				{
					string firstpost_type = FieldValue(FirstPostDate.Items, "StudyFirstPostDateType");
					if (firstpost_type != "Anticipated")
					{
						string firstpost_date = FieldValue(FirstPostDate.Items, "StudyFirstPostDate");
						firstpost = DateHelpers.GetDateParts(firstpost_date);
						if (firstpost_type.ToLower() == "estimate") firstpost.date_string += " (est.)";
					}
				}

				StructType ResultsPostDate = FindStruct(status_items, "ResultsFirstPostDateStruct");
				if (ResultsPostDate != null)
				{
					string results_type = FieldValue(ResultsPostDate.Items, "ResultsFirstPostDateType");
					if (results_type != "Anticipated")
					{
						string resultspost_date = FieldValue(ResultsPostDate.Items, "ResultsFirstPostDate");
						resultspost = DateHelpers.GetDateParts(resultspost_date);
						if (results_type.ToLower() == "estimate") resultspost.date_string += " (est.)";
					}
				}

				StructType LastUpdateDate = FindStruct(status_items, "LastUpdatePostDateStruct");
				if (LastUpdateDate != null)
				{
					string update_type = FieldValue(LastUpdateDate.Items, "LastUpdatePostDateType");
					if (update_type != "Anticipated")
					{
						string updatepost_date = FieldValue(LastUpdateDate.Items, "LastUpdatePostDate");
						updatepost = DateHelpers.GetDateParts(updatepost_date);
						if (update_type.ToLower() == "estimate") updatepost.date_string += " (est.)";
					}
				}

				// expanded access details
				string expanded_access_nctid = StructFieldValue(status_items, "ExpandedAccessInfo", "ExpandedAccessNCTId");
				if (expanded_access_nctid != null)
				{
					relationships.Add(new StudyRelationship(sid, 23, "has an expanded access version", expanded_access_nctid));
					relationships.Add(new StudyRelationship(expanded_access_nctid, 24, "is an expanded access version of", sid));
				}


				// get and store study start date, if available, to use to check possible linked papers
				StructType StudyStartDate = FindStruct(status_items, "StartDateStruct");
				if (StudyStartDate != null)
				{
					string studystart_date = FieldValue(StudyStartDate.Items, "StartDate");
					startdate = DateHelpers.GetDateParts(studystart_date);
					s.study_start_year = startdate.year;
					s.study_start_month = startdate.month;
				}

			}
			else
			{
				// something very odd - this data is basic
				return null;
			}



			if (SponsorCollaboratorsModule != null)
			{
				object[] items = SponsorCollaboratorsModule.Items;

				StructType sponsor = FindStruct(items, "LeadSponsor");
				if (sponsor != null)
				{
					string sponsor_candidate = FieldValue(sponsor.Items, "LeadSponsorName");
					if (StringHelpers.FilterOut_Null_OrgNames(sponsor_candidate) != "")
					{
						sponsor_name = StringHelpers.TidyOrgName(sponsor_candidate, sid);
						if (sponsor_name == "[Redacted]") sponsor_name = "(sponsor name redacted in registry record)";
						contributors.Add(new StudyContributor(sid, 54, "Trial Sponsor", null, sponsor_name,
																null, null));
					}
				}

				StructType resp_party = FindStruct(items, "ResponsibleParty");
				if (resp_party != null)
				{
					var rp_items = resp_party.Items;
					string rp_type = FieldValue(rp_items, "ResponsiblePartyType");

					if (rp_type != "Sponsor")
					{
						string rp_name = FieldValue(rp_items, "ResponsiblePartyInvestigatorFullName");
						string rp_title = FieldValue(rp_items, "ResponsiblePartyInvestigatorTitle");
						string rp_affil = FieldValue(rp_items, "ResponsiblePartyInvestigatorAffiliation");
						string rp_oldnametitle = FieldValue(rp_items, "ResponsiblePartyOldNameTitle");
						string rp_oldorg = FieldValue(rp_items, "ResponsiblePartyOldOrganization");

						if (rp_name == null && rp_oldnametitle != null) rp_name = rp_oldnametitle;
						if (rp_affil == null && rp_oldorg != null) rp_affil = rp_oldorg;

						if (rp_name != null && rp_name != "[Redacted]")
						{
							rp_name = StringHelpers.TidyName(rp_name);

							if (rp_type == "Principal Investigator")
							{
								contributors.Add(new StudyContributor(sid, 51, "Study Lead", null, null, 
												rp_name, rp_affil));
							}

							if (rp_type == "Sponsor-Investigator")
							{
								contributors.Add(new StudyContributor(sid, 70, "Sponsor-investigator", null, null,
												rp_name, rp_affil));
							}
						}
					}
				}

				ListType collaborators = FindList(items, "CollaboratorList");
				if (collaborators != null)
				{
					var collabs = collaborators.Items;
					if (collabs.Length > 0)
					{
						for (int i = 0; i < collabs.Length; i++)
						{
							StructType collab = collabs[i] as StructType;
							string collab_candidate = FieldValue(collab.Items, "CollaboratorName");
							if (StringHelpers.FilterOut_Null_OrgNames(collab_candidate) != "")
							{
								string collab_name = StringHelpers.TidyOrgName(collab_candidate, sid);
								contributors.Add(new StudyContributor(sid, 69, "Collaborating organisation", null, collab_name,
															null, null));
							}
						}
					}
				}

			}


			//if (OversightModule != null)
			//{
			// this data not currently extracted
			//}


			if (DescriptionModule != null)
			{
				object[] items = DescriptionModule.Items;
				if (items != null)
				{
					s.brief_description = FieldValue(items, "BriefSummary");
				}
			}


			if (ConditionBrowseModule != null)
			{
				object[] items = ConditionBrowseModule.Items;
				if (items != null)
				{
					ListType condition_list = FindList(items, "ConditionMeshList");
					if (condition_list != null)
					{
						var conditions = condition_list.Items;
						if (conditions.Length > 0)
						{
							for (int i = 0; i < conditions.Length; i++)
							{
								StructType condition_mesh = conditions[i] as StructType;
								var condition_items = condition_mesh.Items;
								string mesh_code = FieldValue(condition_items, "ConditionMeshId");
								string mesh_term = FieldValue(condition_items, "ConditionMeshTerm");
								topics.Add(new StudyTopic(sid, 13, "condition", mesh_term, 14, "MESH", mesh_code, "browse list"));
							}
						}
					}
				}
			}


			if (InterventionBrowseModule != null)
			{
				object[] items = InterventionBrowseModule.Items;
				if (items != null)
				{
					ListType intervention_list = FindList(items, "InterventionMeshList");
					if (intervention_list != null)
					{
						var interventions = intervention_list.Items;
						if (interventions.Length > 0)
						{
							for (int i = 0; i < interventions.Length; i++)
							{
								StructType intervention_mesh = interventions[i] as StructType;
								var intervention_items = intervention_mesh.Items;
								string mesh_code = FieldValue(intervention_items, "InterventionMeshId");
								string mesh_term = FieldValue(intervention_items, "InterventionMeshTerm");
								topics.Add(new StudyTopic(sid, 12, "chemical / agent", mesh_term, 14, "MESH", mesh_code, "browse list"));
							}
						}
					}
				}
			}


			if (ConditionsModule != null)
			{
				object[] items = ConditionsModule.Items;
				if (items != null)
				{
					ListType condition_list = FindList(items, "ConditionList");
					if (condition_list != null)
					{
						var conditions = condition_list.Items;
						if (conditions.Length > 0)
						{
							for (int i = 0; i < conditions.Length; i++)
							{
								string condition_name = (conditions[i] as FieldType).Value;
								// only add the condition name if not already present in the mesh coded conditions
								if (condition_is_new(condition_name.ToLower()))
								{
									topics.Add(new StudyTopic(sid, 13, "condition", condition_name,  null, "conditions module"));
								}
							}
						}
					}

					ListType keyword_list = FindList(items, "KeywordList");
					if (keyword_list != null)
					{
						var kwords = keyword_list.Items;
						if (kwords.Length > 0)
						{
							for (int i = 0; i < kwords.Length; i++)
							{
								string keyword_name = (kwords[i] as FieldType).Value;
								topics.Add(new StudyTopic(sid, 11, "keyword", keyword_name, null, "conditions module"));
							}
						}
					}
				}
			}


			bool condition_is_new(string condition_name)
			{
				bool new_term = true;
				foreach (StudyTopic k in topics)
				{
					if (k.topic_value.ToLower() == condition_name)
					{
						new_term = false;
						break;
					}
				}
				return new_term;
			}


			if (DesignModule != null)
			{
				object[] items = DesignModule.Items;
				if (items != null)
				{
					s.study_type = FieldValue(items, "StudyType");
					s.study_type_id = TypeHelpers.GetTypeId(s.study_type);

					if (s.study_type == "Interventional")
					{

						ListType phase_list = FindList(items, "PhaseList");
						if (phase_list != null)
						{
							var phases = phase_list.Items;
							if (phases.Length > 0)
							{
								string this_phase = "";
								for (int p = 0; p < phases.Length; p++)
								{
									this_phase = (phases[p] as FieldType).Value;
									features.Add(new StudyFeature(sid, 20, "phase", TypeHelpers.GetPhaseId(this_phase), this_phase));
								}
							}
						}
						else
						{
							features.Add(new StudyFeature(sid, 20, "phase", TypeHelpers.GetPhaseId("Not provided"), "Not provided"));
						}


						StructType design_info = FindStruct(items, "DesignInfo");
						if (design_info != null)
						{
							var design_items = design_info.Items;

							string design_allocation = FieldValue(design_items, "DesignAllocation") ?? "Not provided";
							features.Add(new StudyFeature(sid, 22, "allocation type", TypeHelpers.GetAllocationTypeId(design_allocation), design_allocation));

							string design_intervention_model = FieldValue(design_items, "DesignInterventionModel") ?? "Not provided";
							features.Add(new StudyFeature(sid, 23, "intervention model", TypeHelpers.GetDesignTypeId(design_intervention_model), design_intervention_model));

							string design_primary_purpose = FieldValue(design_items, "DesignPrimaryPurpose") ?? "Not provided";
							features.Add(new StudyFeature(sid, 21, "primary purpose", TypeHelpers.GetPrimaryPurposeId(design_primary_purpose), design_primary_purpose));

							StructType masking_details = FindStruct(design_items, "DesignMaskingInfo");
							if (masking_details != null)
							{
								var masking_items = masking_details.Items;
								string design_masking = FieldValue(masking_items, "DesignMasking") ?? "Not provided";
								features.Add(new StudyFeature(sid, 24, "masking", TypeHelpers.GetMaskingTypeId(design_masking), design_masking));
							}
							else
							{
								features.Add(new StudyFeature(sid, 24, "masking", TypeHelpers.GetMaskingTypeId("Not provided"), "Not provided"));
							}
						}
					}


					if (s.study_type == "Observational")
					{
						string patient_registry = FieldValue(items, "PatientRegistry");
						if (patient_registry == "Yes")  // change type...
						{
							s.study_type_id = 13;
							s.study_type = "Observational Patient Registry";
						}

						StructType design_info = FindStruct(items, "DesignInfo");
						if (design_info != null)
						{
							var design_items = design_info.Items;

							ListType obsmodel_list = FindList(design_items, "DesignObservationalModelList");
							if (obsmodel_list != null)
							{
								var obsmodels = obsmodel_list.Items;
								if (obsmodels.Length > 0)
								{
									string this_obsmodel = "";
									for (int p = 0; p < obsmodels.Length; p++)
									{
										this_obsmodel = (obsmodels[p] as FieldType).Value;
										features.Add(new StudyFeature(sid, 30, "observational model", TypeHelpers.GetObsModelTypeId(this_obsmodel), this_obsmodel));
									}
								}
							}
							else
							{
								features.Add(new StudyFeature(sid, 30, "observational model", TypeHelpers.GetObsModelTypeId("Not provided"), "Not provided"));
							}


							ListType timepersp_list = FindList(design_items, "DesignTimePerspectiveList");
							if (timepersp_list != null)
							{
								var timepersps = timepersp_list.Items;
								if (timepersps.Length > 0)
								{
									string this_persp = "";
									for (int p = 0; p < timepersps.Length; p++)
									{
										this_persp = (timepersps[p] as FieldType).Value;
										features.Add(new StudyFeature(sid, 31, "time perspective", TypeHelpers.GetTimePerspectiveId(this_persp), this_persp));
									}
								}
							}
							else
							{
								features.Add(new StudyFeature(sid, 31, "time perspective", TypeHelpers.GetTimePerspectiveId("Not provided"), "Not provided"));
							}
						}

						StructType biospec_details = FindStruct(items, "BioSpec");
						if (biospec_details != null)
						{
							var biospec_items = biospec_details.Items;
							string biospec_retention = FieldValue(biospec_items, "BioSpecRetention") ?? "Not provided";
							features.Add(new StudyFeature(sid, 32, "biospecimens retained", TypeHelpers.GetSpecimentRetentionId(biospec_retention), biospec_retention));
						}

					}


					StructType enrol_details = FindStruct(items, "EnrollmentInfo");
					if (enrol_details != null)
					{
						var enrol_items = enrol_details.Items;
						string enrolment_count = FieldValue(enrol_items, "EnrollmentCount") ?? "Not provided";
						if (enrolment_count != "Not provided")
						{
							if (Int32.TryParse(enrolment_count, out int enrolment))
							{
								if (enrolment <= 1000 || !Regex.Match(enrolment_count, @"^9+$").Success )
								{
									s.study_enrolment = enrolment;
								}
							}
						}
					}
				}
			}


			//if (ArmsInterventionsModule != null)
			//{
			// this data not currently extracted
			//}

			//if (OutcomesModule != null)
			//{
			// this data not currently extracted
			//}


			if (EligibilityModule != null)
			{
				object[] items = EligibilityModule.Items;
				if (items != null)
				{
					s.study_gender_elig = FieldValue(items, "Gender") ?? "Not provided";
					if (s.study_gender_elig == "All")
					{
						s.study_gender_elig = "Both";
					}
					s.study_gender_elig_id = TypeHelpers.GetGenderEligId(s.study_gender_elig);

					string min_age = FieldValue(items, "MinimumAge");
					if (min_age != null)
					{
						// split number from time unit
						string LHS = min_age.Trim().Substring(0, min_age.IndexOf(' '));
						string RHS = min_age.Trim().Substring(min_age.IndexOf(' ') + 1);
						if (Int32.TryParse(LHS, out int minage))
						{
							s.min_age = minage;
							if (!RHS.EndsWith("s")) RHS += "s";
							s.min_age_units = RHS;
							s.min_age_units_id = TypeHelpers.GetTimeUnitsId(RHS);
						}
					}

					string max_age = FieldValue(items, "MaximumAge");
					if (max_age != null)
					{
						string LHS = max_age.Trim().Substring(0, max_age.IndexOf(' '));
						string RHS = max_age.Trim().Substring(max_age.IndexOf(' ') + 1);
						if (Int32.TryParse(LHS, out int maxage))
						{
							s.max_age = maxage;
							if (!RHS.EndsWith("s")) RHS += "s";
							s.max_age_units = RHS;
							s.max_age_units_id = TypeHelpers.GetTimeUnitsId(RHS);
						}
					}
				}
			}


			if (ContactsLocationsModule != null)
			{
				object[] items = ContactsLocationsModule.Items;
				if (items != null)
				{
					ListType official_list = FindList(items, "OverallOfficialList");
					if (official_list != null)
					{
						var officials = official_list.Items;
						if (officials.Length > 0)
						{
							for (int i = 0; i < officials.Length; i++)
							{
								StructType collab = officials[i] as StructType;
								string official_name = FieldValue(collab.Items, "OverallOfficialName");
								if (official_name != null)
								{
									official_name = StringHelpers.TidyName(official_name);
									string official_affiliation = FieldValue(collab.Items, "OverallOfficialAffiliation");
									contributors.Add(new StudyContributor(sid, 51, "Study Lead", null, 
															null, official_name, official_affiliation));
								}
							}
						}
					}
				}
			}


			//if (MiscInfoModule != null)
			//{
			// this data not currently extracted 
			//}
			string object_type = "", object_class = "";
			if (IPDSharingStatementModule != null)
			{

				object[] items = IPDSharingStatementModule.Items;
				if (items != null)
				{
					string IPDSharingDescription = FieldValue(items, "IPDSharingDescription");
					if (IPDSharingDescription != null)
					{
						string sharing_statement = "(As of " + status_verified_date + "): " + IPDSharingDescription;
						string IPDSharingTimeFrame = FieldValue(items, "IPDSharingTimeFrame") ?? "";
						string IPDSharingAccessCriteria = FieldValue(items, "IPDSharingAccessCriteria") ?? "";
						string IPDSharingURL = FieldValue(items, "IPDSharingURL") ?? "";

						if (IPDSharingTimeFrame != "") sharing_statement += "\r\nTime frame: " + IPDSharingTimeFrame;
						if (IPDSharingAccessCriteria != "") sharing_statement += "\r\nAccess Criteria: " + IPDSharingAccessCriteria;
						if (IPDSharingURL != "") sharing_statement += "\r\nURL: " + IPDSharingURL;

						ListType IPDSharingInfoTypeList = FindList(items, "IPDSharingInfoTypeList");
						if (IPDSharingInfoTypeList != null)
						{	
							var item_types = IPDSharingInfoTypeList.Items;
							if (item_types.Length > 0)
							{	
								string itemlist = "";
								for (int i = 0; i < item_types.Length; i++)
								{
									string item_type = ((FieldType)item_types[i]).Value;
									itemlist += (i == 0) ? item_type : ", " + item_type;
								}
								sharing_statement += "\r\nInformation available: " + itemlist;
							}
						}
						s.data_sharing_statement = sharing_statement;
					}
				}
			}


			#region Establish Linked Data Objects

			/********************* Linked Data Object Data **********************************/
	
			string title_base = "";
			int title_type_id = 0;
			string title_type = "";
			string url = "";

			// this used for specific additional objects from GSK
			string gsk_access_details = "Following receipt of a signed Data Sharing Agreement (DSA), ";
			gsk_access_details += "researchers are provided access to anonymized patient-level data and supporting documentation in a ";
			gsk_access_details += "secure data access system, known as the SAS Clinical Trial Data Transparency (CTDT) system. ";
			gsk_access_details += " GSK may provide data directly to researchers where they are assured that the data will be secure";

			// this used for specific additional objects from Servier
			string servier_access_details = "Servier will provide anonymized patient - level and study-level clinical trial data in response to ";
			servier_access_details += "scientifically valid research proposals. Qualified scientific or medical researchers can submit a research ";
			servier_access_details += "proposal to Servier after registering on the site. If the request is approved and before the transfer of data, ";
			servier_access_details += "a so-called Data Sharing Agreement will have to be signed with Servie";


			// set up initial registry entry data objects 
			// establish base for title
			if (brief_title != null)
			{
				title_base = brief_title;
				title_type_id = 22;
				title_type = "Study short name :: object type";
			}
			else if (official_title != null)
			{
				title_base = official_title;
				title_type_id = 24;
				title_type = "Study scientific name :: object type";
			}
			else
			{
				title_base = sid;
				title_type_id = 26;
				title_type = "Study registry ID :: object type";
			}

			// first object is the protocol registration
			// title will be display title as well
			string object_display_title = title_base + " :: CTG Registry entry";

			// create hash Id for the data object
			string sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

			int object_type_id = 13, object_class_id = 23;

			data_objects.Add(new DataObject(sd_oid, sid, object_display_title, firstpost.year,
								23, "Text", 13, "Trial Registry entry", 100120, 
								"ClinicalTrials.gov", 12, download_datetime));

			// add in title
			object_titles.Add(new ObjectTitle(sd_oid, object_display_title, title_type_id, title_type, true));

			// add in dates
			if (firstpost != null)
			{
				object_dates.Add(new ObjectDate(sd_oid, 12, "Available",
										firstpost.year, firstpost.month, firstpost.day, firstpost.date_string));
			}
			if (updatepost != null)
			{
				object_dates.Add(new ObjectDate(sd_oid, 18, "Updated",
										updatepost.year, updatepost.month, updatepost.day, updatepost.date_string));
			}

			// add in instance
			url = "https://clinicaltrials.gov/ct2/show/study/" + sid;
			object_instances.Add(new ObjectInstance(sd_oid, 100120, "ClinicalTrials.gov", url, true, 
				                      39, "Web text with XML or JSON via API"));


			// if present, set up results data object
			if (resultspost != null && results_data_present)
			{
				object_display_title = title_base + " :: CTG Results entry";
				sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

				data_objects.Add(new DataObject(sd_oid, sid, object_display_title, resultspost.year,
									23, "Text", 28, "Trial registry results summary", 100120, 
									"ClinicalTrials.gov", 12, download_datetime));

				// add in title
				object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
								title_type_id, title_type, true));

				// add in dates
				if (resultspost != null)
				{
					object_dates.Add(new ObjectDate(sd_oid, 12, "Available",
											resultspost.year, resultspost.month, resultspost.day, resultspost.date_string));
				}
				if (updatepost != null)
				{
					object_dates.Add(new ObjectDate(sd_oid, 18, "Updated",
											updatepost.year, updatepost.month, updatepost.day, updatepost.date_string));
				}

				// add in instance
				url = "https://clinicaltrials.gov/ct2/show/results/" + sid;
				object_instances.Add(new ObjectInstance(sd_oid, 100120, "ClinicalTrials.gov", url, true, 
					                               39, "Web text with XML or JSON via API"));

			}


			if (LargeDocumentModule != null)
			{
				object[] items = LargeDocumentModule.Items;
				if (items != null)
				{
					ListType large_doc_list = FindList(items, "LargeDocList");
					if (large_doc_list != null)
					{
						var largedocs = large_doc_list.Items;
						if (largedocs.Length > 0)
						{
							for (int i = 0; i < largedocs.Length; i++)
							{
								StructType large_doc = largedocs[i] as StructType;
								var doc_items = large_doc.Items;
								string type_abbrev = FieldValue(doc_items, "LargeDocTypeAbbrev");
								string has_protocol = FieldValue(doc_items, "LargeDocHasProtocol");
								string has_sap = FieldValue(doc_items, "LargeDocHasSAP");
								string has_icf = FieldValue(doc_items, "LargeDocHasICF");
								string doc_label = FieldValue(doc_items, "LargeDocLabel");
								string doc_date = FieldValue(doc_items, "LargeDocDate");
								string upload_date = FieldValue(doc_items, "LargeDocUploadDate");
								string file_name = FieldValue(doc_items, "LargeDocFilename");

								// create a new data object

								// decompose the doc date to get publication year
								SplitDate docdate = null;
								if (doc_date != null)
								{
									docdate = DateHelpers.GetDateParts(doc_date);
								}

								switch (type_abbrev)
								{
								case "Prot":
									{
										object_type_id = 11; object_type = "Study Protocol";
										break;
									}
								case "SAP":
									{
										object_type_id = 22; object_type = "Statistical analysis plan";
										break;
									}
								case "ICF":
									{
										object_type_id = 18; object_type = "Informed consent forms";
										break;
									}
								case "Prot_SAP":
									{
										object_type_id = 74; object_type = "Protocol SAP";
										break;
									}
								case "Prot_ICF":
									{
										object_type_id = 75; object_type = "Protocol ICF";
										break;
									}
								case "Prot_SAP_ICF":
									{
										object_type_id = 76; object_type = "Protocol SAP ICF";
										break;
									}
								default:
									{
										object_type_id = 37; object_type = type_abbrev;
										break;
									}
								}

								object_display_title = title_base + " :: " + object_type;
								sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

								data_objects.Add(new DataObject(sd_oid, sid, object_display_title, docdate.year,
								23, "Text", object_type_id, object_type, 100120,
								"ClinicalTrials.gov", 11, download_datetime));

								// add in title
								object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
								title_type_id, title_type, true));

								// add in dates
								if (docdate != null)
								{
									object_dates.Add(new ObjectDate(sd_oid, 15, "Created",
										docdate.year, docdate.month, docdate.day, docdate.date_string));
								}

								if (upload_date != null)
								{
									if (DateTime.TryParseExact(upload_date, "MM/dd/yyyy HH:mm", null, DateTimeStyles.None, out DateTime uploaddate))
									{
										object_dates.Add(new ObjectDate(sd_oid, 12, "Available",
																uploaddate.Year, uploaddate.Month, uploaddate.Day, uploaddate.ToString("yyyy MMM dd")));
									}
								}

								// add in instance
								url = "https://clinicaltrials.gov/ProvidedDocs/" + sid.Substring(sid.Length - 2, 2) + "/" + sid + "/" + file_name;
								object_instances.Add(new ObjectInstance(sd_oid, 100120, "ClinicalTrials.gov", url, true, 11, "PDF"));

							}
						}
					}
				}
			}


			if (ReferencesModule != null)
			{
				// references cannot become datav objects until
				// their dates are checked against the study date
				// this is therefore generating a lkist for the future
				object[] items = ReferencesModule.Items;
				if (items != null)
				{
					ListType reference_list = FindList(items, "ReferenceList");
					if (reference_list != null)
					{
						var refs = reference_list.Items;
						if (refs.Length > 0)
						{
							for (int i = 0; i < refs.Length; i++)
							{
								StructType reference = refs[i] as StructType;
								var ref_items = reference.Items;
								string ref_type = FieldValue(ref_items, "ReferenceType");
								if (ref_type == "result")
								{
									string pmid = FieldValue(ref_items, "ReferencePMID");
									string citation = FieldValue(ref_items, "ReferenceCitation");
									references.Add(new StudyReference(sid, pmid, citation, null, null));
								}

								ListType retraction_list = FindList(ref_items, "RetractionList");
								if (retraction_list != null)
								{
									var retractions = retraction_list.Items;
									if (retractions.Length > 0)
									{
										for (int j = 0; j < retractions.Length; j++)
										{
											StructType retraction = retractions[j] as StructType;
											string retraction_pmid = FieldValue(retraction.Items, "RetractionPMID");
											string retraction_source = FieldValue(retraction.Items, "RetractionSource");
											references.Add(new StudyReference(sid, retraction_pmid, retraction_source, null, "RETRACTION"));
										}
									}
								}
							}
						}
					}

					// some of these may be turnable into data objects available, either
					// directly or after review of requests
					// Others will need to be stored as records for future processing
					ListType avail_ipd_List = FindList(items, "AvailIPDList");
					if (avail_ipd_List != null)
					{
						var avail_ipd_items = avail_ipd_List.Items;
						if (avail_ipd_items.Length > 0)
						{
							for (int i = 0; i < avail_ipd_items.Length; i++)
							{
								StructType avail_ipd = avail_ipd_items[i] as StructType;
								var ipd_items = avail_ipd.Items;
								string ipd_id = FieldValue(ipd_items, "AvailIPDId");
								string ipd_type = FieldValue(ipd_items, "AvailIPDType");
								string ipd_url = FieldValue(ipd_items, "AvailIPDURL");
								string ipd_comment = FieldValue(ipd_items, "AvailIPDComment");

								if (ipd_url.Contains("clinicalstudydatarequest.com"))
								{
									// create a new data object
									switch (ipd_type)
									{
										case "Informed Consent Form":
											{
												object_type_id = 18; object_type = "Informed consent forms";
												break;
											}
										case "Dataset Specification":
											{
												object_type_id = 31; object_type = "Data Dictionary";
												break;
											}
										case "Annotated Case Report Form":
											{
												object_type_id = 30; object_type = "Annotated Data Collection Forms";
												break;
											}
										case "Statistical Analysis Plan":
											{
												object_type_id = 22; object_type = "Statistical analysis plan";
												break;
											}
										case "Individual Participant Data Set":
											{
												object_type_id = 80; object_type = "Individual Participant Data";
												break;
											}
										case "Clinical Study Report":
											{
												object_type_id = 26; object_type = "Clinical Study Report";
												break;
											}
										case "Study Protocol":
											{
												object_type_id = 11; object_type = "Study Protocol";
												break;
											}
									}

									object_class_id = (object_type_id == 80) ? 14 : 23;
									object_class = (object_type_id == 80) ? "Dataset" : "Text";

									int? sponsor_id = null;
									string t_base = "";

									if (sponsor_name == "GlaxoSmithKline" || sponsor_name == "GSK")
									{
										sponsor_id = 100163;
										t_base = "GSK-";
									}
									else
									{
										sponsor_id = null;
										t_base = sponsor_name + "-" ?? ""; 
									}

									if (ipd_id == null)
									{
										t_base = title_base;
										title_type_id = 22; title_type = "Study short name :: object type";
									}
									else
									{
										t_base += ipd_id;
										title_type_id = 20; title_type = "Unique data object title";
									}

									object_display_title = t_base + " :: " + object_type;
									sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

									// add data object
									data_objects.Add(new DataObject(sd_oid, sid, object_display_title, null,
									object_class_id, object_class, object_type_id, object_type, sponsor_id, sponsor_name, 
									17, "Case by case download", gsk_access_details,
									"https://clinicalstudydatarequest.com/Help/Help-How-to-Request-Data.aspx",
									null, download_datetime));

									// add in title
									object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
									title_type_id, title_type, true));
								}

								else if (ipd_url.Contains("servier.com"))
								{
									// create a new data object
									if (ipd_type.ToLower().Contains("study-level clinical trial data"))
									{
										object_type_id = 69; object_type = "Aggregated result dataset";
									}
									else
										switch (ipd_type)
										{
											case "Informed Consent Form":
												{
													object_type_id = 18; object_type = "Informed consent forms";
													break;
												}
											case "Statistical Analysis Plan":
												{
													object_type_id = 22; object_type = "Statistical analysis plan";
													break;
												}
											case "Individual Participant Data Set":
												{
													object_type_id = 80; object_type = "Individual Participant Data";
													break;
												}
											case "Clinical Study Report":
												{
													object_type_id = 26; object_type = "Clinical Study Report";
													break;
												}
											case "Study Protocol":
												{
													object_type_id = 11; object_type = "Study Protocol";
													break;
												}
										}

									object_class_id = (object_type_id == 80 || object_type_id == 69) ? 14 : 23;
									object_class = (object_type_id == 80 || object_type_id == 69) ? "Dataset" : "Text";

									// no name provided (in general)
									object_display_title = title_base + " :: " + object_type;
  						     		sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

									data_objects.Add(new DataObject(sd_oid, sid, object_display_title, null,
									object_class_id, object_class, object_type_id, object_type, 101418, "Servier",
									18, "Case by case on-screen access", servier_access_details,
									"https://clinicaltrials.servier.com/data-request-portal/", null, download_datetime));

									// add in title
   								    title_type_id = 22; title_type = "Study short name :: object type";
									object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
									title_type_id, title_type, true));

								}

								else if (ipd_url.Contains("merck.com"))
								{

									// some of the merck records are direct access to a page
									// with a further link to a pdf, plus other study components

									// the others are indications that the object exists but is not directly available
									// create a new data object
								
									if (ipd_url.Contains("&tab=access"))
									{
										object_type_id = 79; object_type = "CSR Summary";
										object_class_id = 23; object_class = "Text";

										// disregard the other entries - as they lead nowhere
										object_display_title = title_base + " :: " + object_type;
										sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

										data_objects.Add(new DataObject(sd_oid, sid, object_display_title, null,
										object_class_id, object_class, object_type_id, object_type, 
										100165, "Merck Sharp & Dohme", 11, download_datetime));

										// add in title
										title_type_id = 22; title_type = "Study short name :: object type";
										object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
										title_type_id, title_type, true));
										
										// add in instance
										object_instances.Add(new ObjectInstance(sd_oid, 4, "Summary version", 100165, 
													"Merck Sharp & Dohme Corp.", ipd_url, true, 11, "PDF", null, null));

									}
								}

								/*
								else if (ipd_url.Contains("biolincc.nhlbi.nih.gov"))
								{

									// for now use the data in CT gov,
									// though later would be better to use the data on BioLINCC itself
									do_id++;

									object_title = title_base + " :: " + object_type;

									DataObject doc_object = new DataObject(sdid, do_id, object_title, null,
									object_class_id, object_class, object_type_id, object_type, 100120, "ClinicalTrials.gov", 11);

									// add in title
									object_titles.Add(new DataObjectTitle(sdid, do_id, object_title,
									title_type_id, title_type, true));
								}
								*/


								else
								{
									ipd_info.Add(new AvailableIPD(sid, ipd_id, ipd_type, ipd_url, ipd_comment));
								}
							}
						}
					}
	 
					// at the moment these records are for storage and  future processing
					// tidy up urls, remove a smnall proportion of obvious nn-useful links
					ListType see_also_list = FindList(items, "SeeAlsoLinkList");
					if (see_also_list != null)
					{
						var see_also_refs = see_also_list.Items;
						if (see_also_refs.Length > 0)
						{
							for (int i = 0; i < see_also_refs.Length; i++)
							{
								StructType see_also = see_also_refs[i] as StructType;
								string link_label = FieldValue(see_also.Items, "SeeAlsoLinkLabel");
								string link_url = FieldValue(see_also.Items, "SeeAlsoLinkURL");

								if (link_url != null)
								{
									bool add_to_db = true;
									if (link_url.EndsWith("/")) link_url = link_url.Substring(0, link_url.Length - 1);

									if (link_label == "NIH Clinical Center Detailed Web Page" && link_url.EndsWith(".html"))
									{
										// add new data object
										object_type_id = 38; object_type = "Study Overview";
										object_class_id = 23; object_class = "Text";

										// disregard the other entries - as they lead nowhere
										object_display_title = title_base + " :: " + object_type;
										sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

										data_objects.Add(new DataObject(sid, sd_oid, object_display_title, null,
										object_class_id, object_class, object_type_id, object_type, 100360, 
										"National Institutes of Health Clinical Center", 11, download_datetime));

										// add in title
										title_type_id = 22; title_type = "Study short name :: object type";
										object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
										title_type_id, title_type, true));

										// add in instance
										object_instances.Add(new ObjectInstance(sd_oid, 100360, "National Institutes of Health Clinical Center",
													link_url, true, 35, "Web text"));

										add_to_db = false;
									}


									if (link_url.Contains("filehosting.pharmacm.com/Download"))
									{
										// add new data object

										string test_url = link_url.ToLower();
										object_type_id = 0;
										int instance_type_id = 1; // default
										string instance_type = "Full resource"; // default

										if (test_url.Contains("csr") || (test_url.Contains("study") && test_url.Contains("report")))
										{
											if (test_url.Contains("redacted"))
											{
												object_type_id = 27; object_type = "Redacted Clinical Study Report";
												instance_type_id = 5; instance_type = "Redacted version";
											}
											else if (test_url.Contains("summary"))
											{
												object_type_id = 79; object_type = "CSR Summary";
												instance_type_id = 4; instance_type = "Summary version";
											}
											else
											{
												object_type_id = 26; object_type = "Clinical Study Report";
											}
										}

										else if (test_url.Contains("csp") || test_url.Contains("protocol"))
										{
											if (test_url.Contains("redacted"))
											{
												object_type_id = 42; object_type = "Redacted Protocol";
												instance_type_id = 5; instance_type = "Redacted version";
											}
											else
											{
												object_type_id = 11; object_type = "Study Protocol";
											}
										}

										else if (test_url.Contains("sap") || test_url.Contains("analysis"))
										{
											if (test_url.Contains("redacted"))
											{
												object_type_id = 43; object_type = "Redacted SAP";
												instance_type_id = 5; instance_type = "Redacted version";
											}
											else
											{
												object_type_id = 22; object_type = "Statistical analysis plan";
											}
										}

										else if (test_url.Contains("summary") || test_url.Contains("rds") || test_url.Contains("ref"))
										{
											object_type_id = 79; object_type = "CSR Summary";
											instance_type_id = 4; instance_type = "Summary version";
										}

										else if (test_url.Contains("poster"))
										{
											object_type_id = 108; object_type = "Conference Poster";
										}

										if (object_type_id > 0 && sponsor_name != null)
										{
											object_class_id = 23; object_class = "Text";
											object_display_title = title_base + " :: " + object_type;
											sd_oid = HashHelpers.CreateMD5(sid + object_display_title);

											DataObject doc_object = new DataObject(sid, sd_oid, object_display_title, null,
											23, "Text", object_type_id, object_type, null, sponsor_name, 11, download_datetime);

											// add data object
											data_objects.Add(doc_object);

											// add in title
											title_type_id = 22; title_type = "Study short name :: object type";
											object_titles.Add(new ObjectTitle(sd_oid, object_display_title,
											title_type_id, title_type, true));

											// add in instance
											object_instances.Add(new ObjectInstance(sd_oid, instance_type_id, instance_type, 
												101419, "TrialScope Disclose", link_url, true, 11, "PDF", null, null));

											add_to_db = false;
										}
									}

									if (link_label == "To obtain contact information for a study center near you, click here.") add_to_db = false;
									if (link_label == "Researchers can use this site to request access to anonymised patient level data and/or supporting documents from clinical studies to conduct further research.") add_to_db = false;
									if (link_label == "University of Texas MD Anderson Cancer Center Website") add_to_db = false;
									if (link_label == "UT MD Anderson Cancer Center website") add_to_db = false;
									if (link_label == "Clinical Trials at Novo Nordisk") add_to_db = false;
									if (link_label == "Memorial Sloan Kettering Cancer Center") add_to_db = false;
									if (link_label == "AmgenTrials clinical trials website") add_to_db = false;
									if (link_label == "Mayo Clinic Clinical Trials") add_to_db = false;
									if (link_url == "http://trials.boehringer-ingelheim.com") add_to_db = false;

									if ((link_label == null || link_label == "") && (link_url.EndsWith(".com") || link_url.EndsWith(".org"))) add_to_db = false;

									if (add_to_db)
									{
										if (link_label != null)
										{
											link_label = link_label.Trim();
											if (link_label.StartsWith("\"") && link_label.EndsWith("\"")) link_label = link_label.Substring(1, link_label.Length - 2);
											if (link_label.StartsWith("((") && link_label.EndsWith("))")) link_label = link_label.Substring(2, link_label.Length - 4);
											if (link_label.StartsWith("(") && link_label.EndsWith(")")) link_label = link_label.Substring(1, link_label.Length - 2);
											if (link_label.StartsWith("|")) link_label = link_label.Substring(1, link_label.Length - 1);
											if (link_label.StartsWith(".")) link_label = link_label.Substring(1, link_label.Length - 1);
											if (link_label.StartsWith(":")) link_label = link_label.Substring(1, link_label.Length - 1);
											link_label = link_label.Trim();
										}
										studylinks.Add(new StudyLink(sid, link_label, link_url));
									}
								}
							}
						}
					}
				}
			}




			#endregion



			s.identifiers = identifiers;
			s.titles = titles;
			s.contributors = contributors;
			s.references = references;
			s.studylinks = studylinks;
			s.ipd_info = ipd_info;
			s.topics = topics;
			s.features = features;
			s.relationships = relationships;

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

			if (s.studylinks.Count > 0)
			{
				repo.StoreStudyLinks(StudyCopyHelpers.study_links_helper,
										  s.studylinks);
			}

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

			if (s.relationships.Count > 0)
			{
				repo.StoreStudyRelationships(StudyCopyHelpers.study_relationship_helper,
										  s.relationships);
			}

			if (s.ipd_info.Count > 0)
			{
				repo.StoreStudyIpdInfo(StudyCopyHelpers.study_ipd_copyhelper,
										  s.ipd_info);
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


		#region Helper Functions

		StructType FindModule(StructType[] items, string nameToMatch)
		{
			StructType a = null;
			foreach (StructType s in items)
			{
				if (s.Name == nameToMatch)
				{
					a = s;
					break;
				}
			}
			return a;
		}

		bool CheckModuleExists(StructType[] items, string nameToMatch)
		{
			bool a = false;
			foreach (StructType s in items)
			{
				if (s.Name == nameToMatch)
				{
					a = true;
					break;
				}
			}
			return a;
		}

		ListType FindList(object[] items, string nameToMatch)
		{
			ListType a = null;
			foreach (object b in items)
			{
				if (b is ListType lb)
				{
					if (lb.Name == nameToMatch)
					{
						a = lb;
						break;
					}
				}
			}
			return a;
		}

		StructType FindStruct(object[] items, string nameToMatch)
		{
			StructType a = null;
			foreach (object b in items)
			{
				if (b is StructType sb)
				{
					if (sb.Name == nameToMatch)
					{
						a = sb;
						break;
					}
				}
			}
			return a;
		}

		string FieldValue(object[] items, string nameToMatch)
		{
			string a = null;
			foreach (object b in items)
			{
				if (b is FieldType fb)
				{
					if (fb.Name == nameToMatch)
					{
						a = fb.Value;
						break;
					}
				}
			}
			return a;
		}

		string StructFieldValue(object[] items, string structToMatch, string fieldToMatch)
		{
			string a = null;
			foreach (object b in items)
			{
				if (b is StructType sb)
				{
					if (sb.Name == structToMatch)
					{
						object[] structItems = sb.Items;
						foreach (object c in structItems)
						{
							if (c is FieldType fc)
							{
								if (fc.Name == fieldToMatch)
								{
									a = fc.Value;
									break;
								}

							}
						}
					}
				}
			}
			return a;
		}

		StructType[] ListStructs(object[] items, string listToMatch)
		{
			StructType[] a = null;
			foreach (object b in items)
			{
				if (b is ListType lb)
				{
					if (lb.Name == listToMatch)
					{
						a = Array.ConvertAll(lb.Items, item => (StructType)item);
						break;
					}
				}
			}
			return a;
		}

		#endregion

	}

}
