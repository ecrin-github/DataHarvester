using System.Collections.Generic;

namespace DataHarvester.biolincc
{
    public class BioLinccIdentifierProcessor
    {
        public void ProcessData(BioLincc_Record st, IStorageDataLayer storage_repo, IMonitorDataLayer mon_repo)
        {
            List<StudyIdentifier> study_identifiers = new List<StudyIdentifier>();
            string sid = st.sd_sid;
            string hbli_identifier = st.accession_number;

            // identifier type = NHBLI ID, id = 42, org = National Heart, Lung, and Blood Institute, id = 100167.
            study_identifiers.Add(new StudyIdentifier(sid, hbli_identifier, 42, "NHLBI ID", 100167, "National Heart, Lung, and Blood Institute (US)"));

            // If there is a NCT ID (there usually is...).

            if (st.registry_ids.Count > 0)
            {
                foreach (RegistryId reg_id in st.registry_ids)
                {
                    study_identifiers.Add(new StudyIdentifier(sid, reg_id.nct_id, 11, "Trial Registry ID", 100120, "ClinicalTrials.gov"));
                }
            }

            // store study identifiers
            if (study_identifiers.Count > 0)
            {
                StudyCopyHelpers sch = new StudyCopyHelpers();
                storage_repo.StoreStudyIdentifiers(sch.study_ids_helper, study_identifiers);
            }

        }

        public void CreateMultNCTsTable(BioLinccDataLayer biolincc_repo)
        {
            biolincc_repo.CreateMultNCTsTable();
        }
            
        public void CreateMultHBLIsTable(BioLinccDataLayer biolincc_repo)
        {
            biolincc_repo.CreateMultHBLIsTable();
        }
    }
}
