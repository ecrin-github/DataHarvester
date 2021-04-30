using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Serilog;

namespace DataHarvester
{
    public class HtmlHelpers
    {
        //private readonly IMonitorDataLayer mon_repo;
        private readonly ILogger _logger;

        public HtmlHelpers(ILogger logger)
        {
            _logger = logger; 
        }

        public string replace_tags(string input_string)
        {
            try
            {
                if (input_string == null)
                {
                    return null;
                }

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
                    if (end_string != -1) // would indicate a non matched sub entry
                    {
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
                    else
                    {
                        // drop any that are left (to get out of the loop)
                        output_string = output_string.Replace("</sub>", "");
                        output_string = output_string.Replace("<sub>", "");
                    }
                }

                while (output_string.Contains("<sup>"))
                {
                    int start_pos = output_string.IndexOf("<sup>");
                    int start_string = start_pos + 5;
                    int end_string = output_string.IndexOf("</sup>", start_string);
                    if (end_string != -1) // would indicate a non matched sup entry
                    {
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
                    else
                    {
                        // drop any that are left  (to get out of the loop)
                        output_string = output_string.Replace("</sup>", "");
                        output_string = output_string.Replace("<sup>", "");
                    }
                }
                return output_string;
            }

            catch(Exception e)
            {
                _logger.Error("In replace_tags: " + e.Message +" (Input was '" + input_string + "')");
                return null;
            }
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
            if (input_string == null)
            {
                return false;
            }
            
            // Check for exit tags as initial tags may have additional attributes.

            if (input_string.Contains("</b>") || input_string.Contains("</i>") || input_string.Contains("</em>") ||
                input_string.Contains("</u>") || input_string.Contains("<br>") || input_string.Contains("<br/>") ||
                input_string.Contains("</p>") || input_string.Contains("</li>") || input_string.Contains("</strong>")
                || input_string.Contains("</a>") )
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

        public async Task CheckURLsAsync(List<ObjectInstance> web_resources)
        {
            HttpClient Client = new HttpClient();
            DateTime today = DateTime.Today;
            foreach (ObjectInstance i in web_resources)
            {
                if (i.resource_type_id == 11)  // just do the study docs for now (pdfs)
                {
                    string url_to_check = i.url;
                    if (url_to_check != null && url_to_check != "")
                    {
                        HttpRequestMessage http_request = new HttpRequestMessage(HttpMethod.Head, url_to_check);
                        var result = await Client.SendAsync(http_request);
                        if ((int)result.StatusCode == 200)
                        {
                            i.url_last_checked = today;
                        }
                    }
                }
            }
        }

        public async Task<bool> CheckURLAsync(string url_to_check)
        {
            HttpClient Client = new HttpClient();
            DateTime today = DateTime.Today;
            if (!string.IsNullOrEmpty(url_to_check))
            {
                try
                {
                    HttpRequestMessage http_request = new HttpRequestMessage(HttpMethod.Head, url_to_check);
                    var result = await Client.SendAsync(http_request);
                    return ((int)result.StatusCode == 200);
                }
                catch (Exception e)
                {
                    string message = e.Message;
                    return false;
                }
            }
            else
            {
                return false;
            }
        }


    }
}
