using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.DBHelpers
{
    class LanguageDataHelper
    {
        // This class uses a SQL statement (rather than extracted data) 
        // to create default language records for each data object
        // The default value is 'en'

        string db_conn;

        public LanguageDataHelper(string _db_conn)
        {
            db_conn = _db_conn;
        }

        public void CreateDefaultLanguageData()
        {
            string sql_string = @"Insert into sd.object_languages
              (sd_id, do_id, lang_code)
              select sd_id, do_id, 'en'
              from sd.data_objects";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }
}
