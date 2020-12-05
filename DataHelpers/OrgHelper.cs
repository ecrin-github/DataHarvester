using Dapper;
using Npgsql;

namespace DataHarvester
{
    public class OrgHelper
    {
        string db_conn;
        LoggingDataLayer logging_repo;

        public OrgHelper(string _db_conn, LoggingDataLayer _logging_repo)
        {
            db_conn = _db_conn;
            logging_repo = _logging_repo;
        }

        public void update_study_identifiers_using_default_name()
        {
            string sql_string = @"update sd.study_identifiers i
            set identifier_org_id = g.id
            from context_ctx.organisations g
            where lower(i.identifier_org) = lower(g.default_name)
            and identifier_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // need to check the possible utility of this....as an additional option
        public void update_study_identifiers_using_default_name_and_suffix()
        {
            string sql_string = @"update sd.study_identifiers i
            set identifier_org_id = g.id
            from context_ctx.organisations g
            where g.display_suffix is not null and g.trim(display_suffix) <> '' 
            and lower(i.identifier_org) =  lower(g.default_name || ' (' || g.display_suffix || ')') 
            and identifier_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_study_identifiers_using_other_name()
        {
            string sql_string = @"update sd.study_identifiers i
            set identifier_org_id = a.org_id
            from context_ctx.org_other_names a
            where lower(i.identifier_org) = lower(a.other_name)
            and identifier_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_study_identifiers_insert_default_names()
        {
            string sql_string = @"update sd.study_identifiers i
            set identifier_org = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end
            from context_ctx.organisations g
            where i.identifier_org_id = g.id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void update_study_contributors_using_default_name()
        {
            string sql_string = @"update sd.study_contributors c
            set organisation_id = g.id
            from context_ctx.organisations g
            where lower(c.organisation_name) = lower(g.default_name)
            and c.organisation_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_study_contributors_using_other_name()
        {
            string sql_string = @"update sd.study_contributors c
            set organisation_id = a.org_id
            from context_ctx.org_other_names a
            where lower(c.organisation_name) = lower(a.other_name)
            and c.organisation_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_study_contributors_insert_default_names()
        {
            string sql_string = @"update sd.study_contributors c
            set organisation_name = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end
            from context_ctx.organisations g
            where c.organisation_id = g.id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_missing_sponsor_ids()
        {
            string sql_string = @"update sd.study_identifiers si
                   set identifier_org_id = sc.organisation_id,
                   identifier_org = sc.organisation_name
                   from sd.study_contributors sc
                   where si.sd_sid = sc.sd_sid
                   and (si.identifier_org ilike 'sponsor' 
                   or si.identifier_org ilike 'company internal')
                   and sc.contrib_type_id = 54";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_data_objects_using_default_name()
        {
            string sql_string = @"update sd.data_objects d
            set managing_org_id = g.id
            from context_ctx.organisations g
            where lower(d.managing_org) = lower(g.default_name)
            and d.managing_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_data_objects_using_other_name()
        {
            string sql_string = @"update sd.data_objects d
            set managing_org_id = a.org_id
            from context_ctx.org_other_names a
            where lower(d.managing_org) = lower(a.other_name)
            and d.managing_org_id is null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void update_data_objects_insert_default_names()
        {
            string sql_string = @"update sd.data_objects d
            set managing_org = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end
            from context_ctx.organisations g
            where d.managing_org_id = g.id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void store_unmatched_study_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.orgs_to_match where source_id = "
            + source_id.ToString() + @" and source_table = 'study_identifiers';
            insert into context_ctx.orgs_to_match (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'study_identifiers', identifier_org, count(identifier_org) 
            from sd.study_identifiers 
            where identifier_org_id is null 
            group by identifier_org; ";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void store_unmatched_study_contributors_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.orgs_to_match where source_id = "
            + source_id.ToString() + @" and source_table = 'study_contributors';
            insert into context_ctx.orgs_to_match (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'study_contributors', organisation_name, count(organisation_name) 
            from sd.study_contributors 
            where organisation_id is null 
            group by organisation_name;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void store_unmatched_data_object_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.orgs_to_match where source_id = "
            + source_id.ToString() + @" and source_table = 'data_objects';
            insert into context_ctx.orgs_to_match (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'data_objects', managing_org, count(managing_org) 
            from sd.data_objects 
            where managing_org_id is null 
            group by managing_org; ";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }

}
