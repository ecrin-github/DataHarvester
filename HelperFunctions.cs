using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace DataHarvester
{
	public class HelperFunctions
	{
		
		public string CleanValue(string inputText, string attribute)
		{
			//// lose the bold and / or italic headings and return
			//// the trimmed content, minus any new lines / carriage returns
			//string attValue = inputText.Replace(attribute, "");
			//if (entrySupp != null)
			//{
			//	attValue = attValue.Replace(entrySupp.InnerText, "");
			//}
			//return attValue.Replace("\n", "").Replace("\r", "").Trim();
			return "";
		}


		public int GetMonthAsInt(string month_name)
		{
			try
			{
				return (int)(Enum.Parse<MonthsFull>(month_name));
			}
			catch (ArgumentException)
			{
				return 0;
			}

		}


		public string GetPMIDFromNLM(string pmc_id)
		{
			var options = new JsonSerializerOptions
			{
				PropertyNameCaseInsensitive = true,
				ReadCommentHandling = JsonCommentHandling.Skip,
				AllowTrailingCommas = true
			};

			string base_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/";
			base_url += "?tool=ECRIN-MDR&email=steve@canhamis.eu&versions=no&ids=";
			string query_url = base_url + pmc_id + "&format=json";

			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(query_url);
			request.Method = "GET";
			WebResponse response = request.GetResponse();

			// assumes response is in utf-8
			MemoryStream ms = new MemoryStream();
			response.GetResponseStream().CopyTo(ms);
			byte[] response_data = ms.ToArray();

			PMCResponse PMC_object = JsonSerializer.Deserialize<PMCResponse>(response_data, options);
			return PMC_object?.records[0]?.pmid;
		}


		public string GetPMIDFromPage(string citation_link)
		{
			string pmid = "";
			/*
			// construct url
			var page = browser.NavigateToPage(new Uri(citation_link));
			// only works with pmid pages, that have this dl tag....
			HtmlNode ids_div = page.Find("dl", By.Class("rprtid")).FirstOrDefault();
			if (ids_div != null)
			{
				HtmlNode[] dts = ids_div.CssSelect("dt").ToArray();
				HtmlNode[] dds = ids_div.CssSelect("dd").ToArray();

				if (dts != null && dds != null)
				{
					for (int i = 0; i < dts.Length; i++)
					{
						string dts_type = dts[i].InnerText.Trim();
						if (dts_type == "PMID:")
						{
							pmid = dds[i].InnerText.Trim();
						}
					}
				}
			}
			*/
			return pmid;
		}


		public string CreateMD5(string input)
		{
			// Use input string to calculate MD5 hash
			using (MD5 md5 = MD5.Create())
			{
				byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
				byte[] hashBytes = md5.ComputeHash(inputBytes);

				// return as base64 string
				// 16 bytes = (5*4) characters + XX==, 
				// 24 rather than 32 hex characters
				return Convert.ToBase64String(hashBytes);

				/*
				// Convert the byte array to hexadecimal string
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < hashBytes.Length; i++)
				{
					sb.Append(hashBytes[i].ToString("X2"));
				}
				return sb.ToString();
				*/
			}
		}
	}


	public class SplitDate
	{
		public int? year;
		public int? month;
		public int? day;
		public string date_string;

		public SplitDate(int? _year, int? _month, int? _day, string _date_string)
		{
			year = _year;
			month = _month;
			day = _day;
			date_string = _date_string;
		}
	}


	public enum MonthsFull
	{
		January = 1, February, March, April, May, June,
		July, August, September, October, November, December
	};


	public enum Months3
	{
		Jan = 1, Feb, Mar, Apr, May, Jun,
		Jul, Aug, Sep, Oct, Nov, Dec
	};
}
