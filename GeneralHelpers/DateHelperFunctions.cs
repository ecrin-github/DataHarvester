using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace DataHarvester
{
	public static class DateHelperFunctions
	{

		public static SplitDate GetDateParts(string dateString)
		{
			// input date string is in the form of "<month name> day, year"
			// or in some cases in the form "<month name> year"
			// split the string on the comma
			string year_string, month_name, day_string;
			int? year_num, month_num, day_num;

			int comma_pos = dateString.IndexOf(',');
			if (comma_pos > 0)
			{
				year_string = dateString.Substring(comma_pos + 1).Trim();
				string first_part = dateString.Substring(0, comma_pos).Trim();

				// first part should split on the space
				int space_pos = first_part.IndexOf(' ');
				day_string = first_part.Substring(space_pos + 1).Trim();
				month_name = first_part.Substring(0, space_pos).Trim();
			}
			else
			{
				int space_pos = dateString.IndexOf(' ');
				year_string = dateString.Substring(space_pos + 1).Trim();
				month_name = dateString.Substring(0, space_pos).Trim();
				day_string = "";
			}

			// convert strings into integers
			if (int.TryParse(year_string, out int y)) year_num = y; else year_num = null;
			month_num = GetMonthAsInt(month_name);
			if (int.TryParse(day_string, out int d)) day_num = d; else day_num = null;
			string month_as3 = ((Months3)month_num).ToString();

			// get date as string
			string date_as_string;
			if (year_num != null && month_num != null && day_num != null)
			{
				date_as_string = year_num.ToString() + " " + month_as3 + " " + day_num.ToString();
			}
			else if (year_num != null && month_num != null && day_num == null)
			{
				date_as_string = year_num.ToString() + ' ' + month_as3;
			}
			else if (year_num != null && month_num == null && day_num == null)
			{
				date_as_string = year_num.ToString();
			}
			else
			{
				date_as_string = null;
			}

			return new SplitDate(year_num, month_num, day_num, date_as_string);
		}


		public static string StandardiseDateFormat(string inputDate)
		{
			SplitDate SD = GetDateParts(inputDate);
			return SD.date_string;
		}



		public static int GetMonthAsInt(string month_name)
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
