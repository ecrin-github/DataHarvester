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
			try
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
					else
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
			catch (Exception e)
			{
				StringHelpers.SendError("In ReplaceApos: " + e.Message + " (input was '" + apos_name + "')");
				return apos_name;
			}

		}

		public static string CheckTitle(string in_title)
		{
			string out_title = "";
			if (!string.IsNullOrEmpty(in_title))
			{
				string lower_title = in_title.ToLower().Trim();
				if (lower_title != "n.a." && lower_title != "na"
					&& lower_title != "n.a" && lower_title != "n/a"
					&& lower_title != "no disponible" && lower_title != "not available")
				{
					if (in_title.Contains("<"))
					{
						out_title = HtmlHelpers.replace_tags(in_title);
						out_title = HtmlHelpers.strip_tags(out_title);
					}
					else
					{
						out_title = in_title;
					}
				}
			}
			return out_title;
		}


		public static string TidyOrgName(string in_name, string sid)
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

				if (name.ToLower().Contains("newcastle") && name.ToLower().Contains("university")
					&& !name.Contains("hospital"))
				{
					if (name.ToLower().Contains("nsw") || name.ToLower().Contains("australia"))
					{
						name = "University of Newcastle (Australia)";
					}
					else if (name.ToLower().Contains("uk") || name.ToLower().Contains("tyne"))
					{
						name = "University of Newcastle (UK)";
					}
					else if (sid.StartsWith("ACTRN"))
					{
						name = "University of Newcastle (Australia)";
					}
					else
					{
						name = "University of Newcastle (UK)";
					}
				}

			}



			return name;
		}


		public static string FilterOut_Null_OrgNames(string in_name)
		{
			string out_name = in_name;
			// in_name should be in lower case...
			if (in_name == "-" || in_name == "n.a." || in_name == "n a" || in_name == "n/a" ||
				in_name == "na" || in_name == "nil" || in_name == "nill" || in_name == "no" || in_name == "non")
			{
				out_name = "";
			}
			else if (in_name.StartsWith("no ") || in_name == "not applicable" || in_name.StartsWith("not prov"))
			{
				out_name = "";
			}
			else if (in_name == "none" || in_name.StartsWith("non fund") || in_name.StartsWith("non spon")
				|| in_name.StartsWith("nonfun") || in_name.StartsWith("noneno"))
			{
				out_name = "";
			}
			else if (in_name.StartsWith("investigator ") || in_name == "investigator" || in_name == "self"
				|| in_name.StartsWith("Organisation name "))
			{
				out_name = "";
			}
			else
			{
				out_name = in_name;
			}

			return out_name;
		}


		public static string TidyORCIDId(string input_identifier)
		{
			string identifier = input_identifier.Replace("https://orcid.org/", "");
			identifier = identifier.Replace("http://orcid.org/", "");
			identifier = identifier.Replace("/", "-");
			identifier = identifier.Replace(" ", "-");
			return identifier;
		}


		public static string TidyORCIDId2(string input_identifier)
		{
			string identifier = input_identifier;
			if (identifier.Length == 16)
			{
				identifier = identifier.Substring(0, 4) + "-" + identifier.Substring(4, 4) +
							"-" + identifier.Substring(8, 4) + "-" + identifier.Substring(12, 4);
			}
			if (identifier.Length == 15) identifier = "0000" + identifier;
			if (identifier.Length == 14) identifier = "0000-" + identifier;

			return identifier;
		}


		public static string lang_3_to_2(string input_lang_code)
		{
			string lang_code = "";
			switch (input_lang_code)
			{
				// covers most of the non English cases

				case "fre": lang_code = "fr"; break;
				case "ger": lang_code = "de"; break;
				case "spa": lang_code = "es"; break;
				case "ita": lang_code = "it"; break;
				case "por": lang_code = "pt"; break;
				case "rus": lang_code = "ru"; break;
				case "tur": lang_code = "tr"; break;
				case "hun": lang_code = "hu"; break;
				case "pol": lang_code = "pl"; break;
				case "swe": lang_code = "sv"; break;
				case "nor": lang_code = "no"; break;
				case "dan": lang_code = "da"; break;
				case "fin": lang_code = "fi"; break;
				default: lang_code = "??"; break;
			}
			return lang_code;
		}

		public static void SendFeedback(string message, string identifier = "")
		{
			string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
			System.Console.WriteLine(dt_string + message + identifier);
		}

		public static void SendHeader(string message)
		{
			string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
			System.Console.WriteLine("");
			System.Console.WriteLine(dt_string + "**** " + message + " ****");
		}

		public static void SendError(string message)
		{
			string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
			System.Console.WriteLine("");
			System.Console.WriteLine("+++++++++++++++++++++++++++++++++++++++");
			System.Console.WriteLine(dt_string + "***ERROR*** " + message);
			System.Console.WriteLine("+++++++++++++++++++++++++++++++++++++++");
			System.Console.WriteLine("");
		}
	}
} 