﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace DataHarvester
{
	public class HtmlHelperFunctions
	{
		public string replace_tags(string input_string)
		{
			string output_string = input_string;
			while (output_string.Contains("<div"))
			{
				// remove any div start tags
				int start_pos = output_string.IndexOf("<div");
				int end_pos = output_string.IndexOf(">", start_pos);
				output_string = output_string.Substring(0, start_pos) + output_string.Substring(end_pos + 1);
			}

			// remove all end divs
			output_string = output_string.Replace("</div>", "");

			while (output_string.Contains("<span"))
			{
				// remove any span start tags
				int start_pos = output_string.IndexOf("<span");
				int end_pos = output_string.IndexOf(">", start_pos);
				output_string = output_string.Substring(0, start_pos) + output_string.Substring(end_pos + 1);
			}

			// remove all end spans
			output_string = output_string.Replace("</span>", "");

			while (output_string.Contains("<sub>"))
			{
				int start_pos = output_string.IndexOf("<sub>");
				int start_string = start_pos + 5;
				int end_string = output_string.IndexOf("</sub>", start_string);
				int end_pos = end_string + 5;
				string string_to_change = output_string.Substring(start_string, end_string - start_string);
				string new_string = "";
				for (int i = 0; i < string_to_change.Length; i++)
				{
					new_string += ChangeToSubUnicode(string_to_change[i]);
				}
				if (end_pos > output_string.Length - 1)
				{
					output_string = output_string.Substring(0, start_pos) + new_string;
				}
				else
				{
					output_string = output_string.Substring(0, start_pos) + new_string + output_string.Substring(end_pos + 1);
				}

			}

			while (output_string.Contains("<sup>"))
			{
				int start_pos = output_string.IndexOf("<sup>");
				int start_string = start_pos + 5;
				int end_string = output_string.IndexOf("</sup>", start_string);
				int end_pos = end_string + 5;
				string string_to_change = output_string.Substring(start_string, end_string - start_string);
				string new_string = "";
				for (int i = 0; i < string_to_change.Length; i++)
				{
					new_string += ChangeToSupUnicode(string_to_change[i]);
				}
				if (end_pos > output_string.Length - 1)
				{
					output_string = output_string.Substring(0, start_pos) + new_string;
				}
				else
				{
					output_string = output_string.Substring(0, start_pos) + new_string + output_string.Substring(end_pos + 1);
				}
			}
			return output_string;
		}


		private char ChangeToSupUnicode(char a)
		{
			char unicode = a;
			switch (a)
			{
				case '0': unicode = '\u2070'; break;
				case '1': unicode = '\u0B09'; break;
				case '2': unicode = '\u0B02'; break;
				case '3': unicode = '\u0B03'; break;
				case '4': unicode = '\u2074'; break;
				case '5': unicode = '\u2075'; break;
				case '6': unicode = '\u2076'; break;
				case '7': unicode = '\u2077'; break;
				case '8': unicode = '\u2078'; break;
				case '9': unicode = '\u2079'; break;
				case 'i': unicode = '\u2071'; break;
				case '+': unicode = '\u207A'; break;
				case '-': unicode = '\u207B'; break;
				case '=': unicode = '\u207C'; break;
				case '(': unicode = '\u207D'; break;
				case ')': unicode = '\u207E'; break;
				case 'n': unicode = '\u207F'; break;
			}
			return unicode;
		}

		private char ChangeToSubUnicode(char a)
		{
			char unicode = a;
			switch (a)
			{
				case '0': unicode = '\u2080'; break;
				case '1': unicode = '\u2081'; break;
				case '2': unicode = '\u2082'; break;
				case '3': unicode = '\u2083'; break;
				case '4': unicode = '\u2084'; break;
				case '5': unicode = '\u2085'; break;
				case '6': unicode = '\u2086'; break;
				case '7': unicode = '\u2087'; break;
				case '8': unicode = '\u2088'; break;
				case '9': unicode = '\u2089'; break;
				case '+': unicode = '\u208A'; break;
				case '-': unicode = '\u208B'; break;
				case '=': unicode = '\u208C'; break;
				case '(': unicode = '\u208D'; break;
				case ')': unicode = '\u208E'; break;
				case 'a': unicode = '\u2090'; break;
				case 'e': unicode = '\u2091'; break;
				case 'o': unicode = '\u2092'; break;
				case 'x': unicode = '\u2093'; break;
				case 'h': unicode = '\u2095'; break;
				case 'k': unicode = '\u2096'; break;
				case 'l': unicode = '\u2097'; break;
				case 'm': unicode = '\u2098'; break;
				case 'n': unicode = '\u2099'; break;
				case 'p': unicode = '\u209A'; break;
				case 's': unicode = '\u209B'; break;
				case 't': unicode = '\u209C'; break;

			}
			return unicode;
		}

		public bool check_for_tags(string input_string)
		{
			if (input_string.Contains("<b>") || input_string.Contains("<i>") || input_string.Contains("<em>") ||
				input_string.Contains("<u>") || input_string.Contains("<b>") || input_string.Contains("<b>") ||
				input_string.Contains("<p>") || input_string.Contains("<li>") || input_string.Contains("<strong>")
				|| input_string.Contains("<a>"))
			{
				return true;
			}
			else
			{
				return false;
			}
		}


		public string strip_tags(string input_string)
		{
			string output_string = input_string.Replace("<ol>", "").Replace("<ul>", "").Replace("</ol>", "").Replace("</ul>", "");
			output_string = output_string.Replace("<li>", "* ").Replace("</li>", "");
			output_string = output_string.Replace("<b>", "").Replace("</b>", "").Replace("<strong>", "").Replace("</strong>", "");
			output_string = output_string.Replace("<i>", "").Replace("</i>", "").Replace("<em>", "").Replace("</em>", "");
			output_string = output_string.Replace("<p>", "").Replace("</p>", "");
			return output_string;
		}
	}
}