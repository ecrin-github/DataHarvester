using System;
using System.Security.Cryptography;

namespace DataHarvester
{
    public class HashHelpers
    {
        LoggingDataLayer logging_repo;

        public HashHelpers(LoggingDataLayer _logging_repo)
        {
            logging_repo = _logging_repo;
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
            }
        }
    }

}
