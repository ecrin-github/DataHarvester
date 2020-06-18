using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
    class IdentifierHelpers
    {
		// A helper function called from the loop that goes through the secondary Id data
		// It tries to make the data as complete as possible, depending on the typem of 
		// secondary id that is being processed
		void GetIdentifierProps(object[] items, out string id_type, out string id_org,
								out int? id_type_id, out int? id_org_id)
		{
			//default values
			StringHelpers sh = new StringHelpers();

			id_type = "SecondaryIdType";
			id_org = sh.TidyPunctuation("SecondaryIdDomain");

			id_type_id = null;
			id_org_id = null;

			if (id_org == null)
			{
				id_org = "No organisation name provided in source data";
				id_org_id = 12;
			}

			if (id_type == null)
			{
				id_type_id = 1;
				id_type = "No type given in source data";
			}

			if (id_type == "Other Identifier")
			{
				id_type_id = 90;
				id_type = "Other";
			}

			if (id_type == "U.S. NIH Grant/Contract")
			{
				id_org_id = 100134;
				id_org = "National Institutes of Health";
				id_type_id = 13;
				id_type = "Funder’s ID";
			}

			if (id_type == "Other Grant/Funding Number")
			{
				id_type_id = 13;
				id_type = "Funder’s ID";
			}

			if (id_type == "EudraCT Number")
			{
				id_org_id = 100123;
				id_org = "EU Clinical Trials Register";
				id_type_id = 11;
				id_type = "Trial Registry ID";
			}

			if (id_type == "Registry Identifier")
			{
				id_type_id = 11;
				id_type = "Trial Registry ID";
				id_org = id_org.ToLower();

				if (id_org.Contains("ctrp") || id_org.Contains("pdq") || id_org.Contains("nci"))
				{
					// NCI CTRP programme
					id_org_id = 100162;
					id_org = "National Cancer Institute";
					id_type_id = 39;
					id_type = "NIH CTRP ID";
				}

				else if (id_org.Contains("daids"))
				{
					// NCI CTRP programme
					id_org_id = 100168;
					id_org = "National Institute of Allergy and Infectious Diseases";
					id_type_id = 40;
					id_type = "DAIDS ID";
				}

				else if (id_org.Contains("who") || id_org.Contains("utn") || id_org.Contains("universal"))
				{
					// NCI CTRP programme
					id_org_id = 100115;
					id_org = "International Clinical Trials Registry Platform";
				}

				else if (id_org.Contains("japic") || id_org.Contains("cti"))
				{
					// japanese registry
					id_org_id = 100157;
					id_org = "Japan Pharmaceutical Information Center";
				}

				else if (id_org.Contains("umin"))
				{
					// japanese registry
					id_org_id = 100156;
					id_org = "University Hospital Medical Information Network CTR";
				}

				else if (id_org.Contains("isrctn"))
				{
					// japanese registry
					id_org_id = 100126;
					id_org = "ISRCTN";
				}

				else if (id_org.Contains("india") || id_org.Contains("ctri"))
				{
					// japanese registry
					id_org_id = 100121;
					id_org = "Clinical Trials Registry - India";
				}

				else if (id_org.Contains("eudract"))
				{
					// japanese registry
					id_org_id = 100123;
					id_org = "EU Clinical Trials Register";
				}

				else if (id_org.Contains("drks") || id_org.Contains("german") || id_org.Contains("deutsch"))
				{
					// japanese registry
					id_org_id = 100124;
					id_org = "Deutschen Register Klinischer Studien";
				}

				else if (id_org.Contains("nederlands") || id_org.Contains("dutch"))
				{
					// japanese registry
					id_org_id = 100132;
					id_org = "The Netherlands National Trial Register";
				}

				else if (id_org.Contains("ansm") || id_org.Contains("agence") || id_org.Contains("rcb"))
				{
					// french asnsm number=
					id_org_id = 101408;
					id_org = "Agence Nationale de Sécurité du Médicament";
					id_type_id = 41;
					id_type = "Regulatory Body ID";
				}


				else if (id_org.Contains("iras") || id_org.Contains("hra"))
				{
					// uk IRAS number
					id_org_id = 101409;
					id_org = "Health Research Authority";
					id_type_id = 41;
					id_type = "Regulatory Body ID";
				}

				else if (id_org.Contains("anzctr") || id_org.Contains("australian"))
				{
					// australian registry
					id_org_id = 100116;
					id_org = "Australian New Zealand Clinical Trials Registry";
				}

				else if (id_org.Contains("chinese"))
				{
					// chinese registry
					id_org_id = 100118;
					id_org = "Chinese Clinical Trial Register";
				}

				else if (id_org.Contains("thai"))
				{
					// thai registry
					id_org_id = 100131;
					id_org = "Thai Clinical Trials Register";
				}

				if (id_org == "JHMIRB" || id_org == "JHM IRB")
				{
					// ethics approval number
					id_org_id = 100190;
					id_org = "Johns Hopkins University";
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}

				if (id_org.ToLower().Contains("ethics") || id_org == "Independent Review Board" || id_org.Contains("IRB"))
				{
					// ethics approval number
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}
			}

			if (id_type_id == 1 || id_type_id == 90)
			{
				string id_value = "SecondaryId";

				if (id_org == "UTN")
				{
					// NCI CTRP programme
					id_org_id = 100115;
					id_org = "International Clinical Trials Registry Platform";
					id_type_id = 11;
					id_type = "Trial Registry ID";
				}

				if (id_org.ToLower().Contains("ansm") || id_org.ToLower().Contains("rcb"))
				{
					// NCI CTRP programme
					id_org_id = 101408;
					id_org = "Agence Nationale de Sécurité du Médicament";
					id_type_id = 41;
					id_type = "Regulatory Body ID";
				}

				if (id_org == "JHMIRB" || id_org == "JHM IRB")
				{
					// ethics approval number
					id_org_id = 100190;
					id_org = "Johns Hopkins University";
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}

				if (id_org.ToLower().Contains("ethics") || id_org == "Independent Review Board" || id_org.Contains("IRB"))
				{
					// ethics approval number
					id_type_id = 12;
					id_type = "Ethics Review ID";
				}

				if (id_value.Length > 4 && id_value.Substring(0, 4) == "NCI-")
				{
					// ethics approval number
					id_org_id = 100162;
					id_org = "National Cancer Institute";
				}

				// need a mechanism here to find the org id and system name for the organisation as given
				// probably better to do it in a bulk operation on transfer of the data to the ad tables
			}
		}
	}
}
