using System;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;
using System.Text;

namespace DataHarvester
{
	public static class StringHelpers
	{

		public static string TidyName(string in_name)
		{

			string name = in_name.Replace(".", "");
			string low_name = name.ToLower();

			if (low_name.StartsWith("professor "))
			{
				name = name.Substring(4, name.Length - 10);
				low_name = name.ToLower();
			}
			else if (low_name.StartsWith("prof "))
			{
				name = name.Substring(5, name.Length - 5);
				low_name = name.ToLower();
			}

			if (low_name.StartsWith("dr ")) { name = name.Substring(3, name.Length - 3); }
			else if (low_name.StartsWith("dr med ")) { name = name.Substring(7, name.Length - 7); }

			int comma_pos = name.IndexOf(',');
			if (comma_pos > -1) { name = name.Substring(0, comma_pos); }

			return name;
		}


		public static string ReplaceApos(string apos_name)
		{
			int apos_pos = apos_name.IndexOf("'");
			int alen = apos_name.Length;
			if (apos_pos != -1)
			{
				if (apos_pos == 0)
				{
					apos_name = "‘" + apos_name.Substring(1);
				}
				else if (apos_pos == alen - 1)
				{
					apos_name = apos_name.Substring(0, alen - 1) + "’";
				}
				{
					if (apos_name[apos_pos - 1] == ' ' || apos_name[apos_pos - 1] == '(')
					{
						apos_name = apos_name.Substring(0, apos_pos) + "‘" + apos_name.Substring(apos_pos + 1, alen - apos_pos - 1);

					}
					else
					{
						apos_name = apos_name.Substring(0, apos_pos) + "’" + apos_name.Substring(apos_pos + 1, alen - apos_pos - 1);
					}
				}
			}
			return apos_name;
		}


		public static string TidyPunctuation(string in_name)
		{
			string name = in_name;
			if (name != null)
			{
				if (name.Contains("."))
				{
					// protect these exceptions to the remove full stop rule
					name = name.Replace(".com", "|com");
					name = name.Replace(".gov", "|gov");
					name = name.Replace(".org", "|org");

					name = name.Replace(".", "");

					name = name.Replace("|com", ".com");
					name = name.Replace("|gov", ".gov");
					name = name.Replace("|org", ".org");
				}

				// do this as a loop as there may be several apostrophes that
				// need replacing to different types of quote
				while (name.Contains("'"))
				{
					name = ReplaceApos(name);
				}
			}
			return name;
		}


		public static string TidyORCIDIdentifier(string input_identifier, string sd_oid, DataLayer repo)
		{
			string identifier = input_identifier.Replace("https://orcid.org/", "");
			identifier = identifier.Replace("http://orcid.org/", "");
			identifier = identifier.Replace("/", "-");
			identifier = identifier.Replace(" ", "-");
			if (identifier.Length != 19)
			{
				string qText = "ORCID identifier for person " + sd_oid + " is " + identifier + " and has non standard length'";
				repo.StoreExtractionNote(sd_oid, 24, qText);
				if (identifier.Length == 16)
				{
					identifier = identifier.Substring(0, 4) + "-" + identifier.Substring(4, 4) +
								"-" + identifier.Substring(8, 4) + "-" + identifier.Substring(12, 4);
				}
				if (identifier.Length == 15) identifier = "0000" + identifier;
				if (identifier.Length == 14) identifier = "0000-" + identifier;
			}
			return identifier;
		}

	}
}
