using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester.DBHelpers
{
    public static class OrgHelper
    {
        public static void update_study_identifiers_using_default_name(string db_conn)
        {
            string sql_string = @"update sd.study_identifiers i
            set identifier_org_id = g.id
            from ctx.organisations g
            where i.identifier_org = g.default_name
            and identifier_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void update_study_identifiers_using_other_name(string db_conn)
        {
            string sql_string = @"update sd.study_identifiers i
            set identifier_org_id = a.org_id
            from ctx.org_other_names a
            where i.identifier_org = a.other_name
            and identifier_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void update_study_identifiers_insert_default_names(string db_conn)
        {
            string sql_string = @"update sd.study_identifiers i
            set identifier_org = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end
            from ctx.organisations g
            where i.identifier_org_id = g.id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }



        public static void update_study_contributors_using_default_name(string db_conn)
        {
            string sql_string = @"update sd.study_contributors c
            set org_id = g.id
            from ctx.organisations g
            where c.org_name = g.default_name
            and c.org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void update_study_contributors_using_other_name(string db_conn)
        {
            string sql_string = @"update sd.study_contributors c
            set org_id = a.org_id
            from ctx.org_other_names a
            where c.org_name = a.other_name
            and c.org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void update_study_contributors_insert_default_names(string db_conn)
        {
            string sql_string = @"update sd.study_contributors c
            set org_name = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end
            from ctx.organisations g
            where c.org_id = g.id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }




        public static void update_data_objects_using_default_name(string db_conn)
        {
            string sql_string = @"update sd.data_objects d
            set managing_org_id = g.id
            from ctx.organisations g
            where d.managing_org = g.default_name
            and d.managing_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void update_data_objects_using_other_name(string db_conn)
        {
            string sql_string = @"update sd.data_objects d
            set managing_org_id = a.org_id
            from ctx.org_other_names a
            where d.managing_org = a.other_name
            and d.managing_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public static void update_data_objects_insert_default_names(string db_conn)
        {
            string sql_string = @"update sd.data_objects d
            set managing_org = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end
            from ctx.organisations g
            where d.managing_org_id = g.id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }
}
