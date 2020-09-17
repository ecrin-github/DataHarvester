using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataHarvester
{
    public class OrgIdHelper
    {
        string db_conn;

        public OrgIdHelper(string _db_conn)
        {
            db_conn = _db_conn;
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


        // update publisher name in citation object
        // using eissn code
        public void update_publisher_names_using_eissn()
        {
            string sql_string = @"with t as (
                                select e.eissn as jcode, p.publisher as pubname
                                from ctx.pub_eids e inner join ctx.publishers p
                                on e.pub_id = p.id
                            )
                            update sd.citation_objects c
                            set publisher_name = t.pubname
                            from t
                            where c.eissn = t.jcode
                            and c.publisher_name is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update publisher name in citation object
        // using pissn code
        public void update_publisher_names_using_pissn()
        {
            string sql_string = @"with t as (
                            select e.pissn as jcode, p.publisher as pubname
                            from ctx.pub_pids e inner join ctx.publishers p
                            on e.pub_id = p.id
                        )
                        update sd.citation_objects c
                        set publisher_name = t.pubname
                        from t
                        where c.pissn = t.jcode
                        and c.publisher_name is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update publisher name in citation object
        // using journal title, for remainer
        // (but are journal titles unique - probably not...)
        public void update_publisher_names_using_journal_names()
        {
            string sql_string = @"with t as (
                            select e.journal_name as jname, p.publisher as pubname
                            from ctx.pub_journals e inner join ctx.publishers p
                            on e.pub_id = p.id
                        )
                        update sd.citation_objects c
                        set publisher_name = t.pubname
                        from t
                        where lower(trim(c.journal_title)) = lower(trim(t.jname))
                        and c.publisher_name is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update the org ids using the publisher name
        // against the organisations table(default name)
        public void update_publisher_ids_usisng_default_names()
        {

            string sql_string = @"with t as (
                            select id as orgid, default_name as orgname
                            from ctx.organisations
                        )
                        update sd.citation_objects c
                        set pub_org_id = t.orgid
                        from t
                        where lower(trim(c.publisher_name)) = lower(trim(t.orgname))
                        and c.pub_org_id is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update the org ids using the publisher name
        // against the org_other_names table (other name)

        public void update_publisher_ids_usisng_other_names()
        {
            string sql_string = @"with t as (
                            select org_id as orgid, other_name as orgname
                            from ctx.org_other_names
                       )
                       update sd.citation_objects c
                       set pub_org_id = t.orgid
                       from t
                       where lower(trim(c.publisher_name)) = lower(trim(t.orgname))
                       and c.pub_org_id is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // then need to update publisher organisation name
        // using the 'official' default name and any suffixin
        // in the organisations table.

        public void update_publisher_names_to_defaults()
        {
            string sql_string = @"update sd.citation_objects c
                            set publisher_name = g.default_name ||
                            case when g.display_suffix is not null and trim(g.display_suffix) <> '' 
                            then ' (' || g.display_suffix || ')'
                            else '' end
                            from ctx.organisations g
                            where c.pub_org_id = g.id;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update the publishers' identifier ids in the
        // object identifiers' table
        // using the updated org_ids in the citation objects table

        public void update_identifiers_publishers_ids()
        {
            string sql_string = @"update sd.object_identifiers
                            set identifier_org_id = c.pub_org_id
                            from sd.citation_objects c
                            where sd.object_identifiers.sd_id = c.sd_id
                            and identifier_type_id = 34;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // Then update the publisher identifier
        // name in the object identifiers table

        public void update_identifiers_publishers_names()
        {
            string sql_string = @"with t as (
                            select id as orgid, default_name as orgname
                            from ctx.organisations
                        )
                        update sd.object_identifiers i
                        set identifier_org = t.orgname
                        from t
                        where i.identifier_org_id = t.orgid
                        and i.identifier_type_id = 34
                        and i.identifier_org_id is not null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void update_identifiers_publishers_names_to_default()
        {
            string sql_string = @"update sd.object_identifiers i
                        set identifier_org = g.default_name ||
                        case when g.display_suffix is not null and trim(g.display_suffix) <> '' 
                             then ' (' || g.display_suffix || ')'
                        else '' end
                        from ctx.organisations g
                        where i.identifier_org_id = g.id
                        and i.identifier_type_id = 34
                        and i.identifier_org_id is not null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // insert topic type id where the name matches a listed topic type

        public void update_topic_types()
        {
            string sql_string = @"with t as (
                            select id as type_id, name as topic_name
                            from lup.topic_types
                        )
                        update sd.object_topics p
                        set topic_type_id = t.type_id
                        from t
                        where p.topic_type = t.topic_name;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // identify geographic terms amongst the topics

        public void update_geographic_topics()
        {
            string sql_string = @"update sd.object_topics p
                                  set topic_type_id = 16,
                                  topic_type = 'geographic'
                                  from ctx.geog_entities g
                                  where p.topic = g.name
                                  and topic_type is null;;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // also
        public void delete_humans_as_topic()
        {
            string sql_string = @"delete from sd.object_topics
                            where topic = 'Humans';";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }






        }
}
