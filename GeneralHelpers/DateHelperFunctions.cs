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
	public class DateHelperFunctions
	{
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
