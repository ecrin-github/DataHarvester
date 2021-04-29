using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
    public class Credentials
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public Credentials GetCredentials(IConfiguration _settings)
        {
            return new Credentials
            {
                Host = _settings["host"],
                Username = _settings["user"],
                Password = _settings["password"]
            };
        }
    }
}
