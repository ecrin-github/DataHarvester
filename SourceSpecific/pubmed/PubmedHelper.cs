﻿using Dapper;
using Npgsql;
using System.Xml.Linq;

namespace DataHarvester
{
    public class PubmedHelper
    {
        string db_conn;

        public PubmedHelper(string _db_conn)
        {
            db_conn = _db_conn;
        }

        // All these used within pubmed file processing

        // These functions make use of the explicit cast operators
        // available for XElement and XAttribute.
        // Most functions include a preliminary check for the existence
        // of the Element or Attribute node itself, followed by a cast to
        // the required type of the Element or Attribute's 
        // Value (= inner HTML for an element).
        /*
        public string GetElementAsString(XElement e) => (e == null) ? null : (string)e;

        public string GetAttributeAsString(XAttribute a) => (a == null) ? null : (string)a;

        public int? GetElementAsInt(XElement e) => (e == null) ? null : (int?)e;

        public int? GetAttributeAsInt(XAttribute a) => (a == null) ? null : (int?)a;

        public bool GetAttributeAsBool(XAttribute a)
        {
            string avalue = GetAttributeAsString(a);
            if (avalue != null)
            {
                return (avalue.ToUpper() == "Y") ? true : false;
            }
            else
            {
                return false;
            }
        }
        */

        /*
        // update publisher name in citation object
        // using eissn code
        public void obtain_publisher_names_using_eissn()
        {
            string sql_string = @"with t as (
                                select e.eissn as jcode, p.publisher as pubname
                                from context_ctx.pub_eids e inner join context_ctx.publishers p
                                on e.pub_id = p.id
                            )
                            update sd.citation_objects c
                            set managing_org = t.pubname
                            from t
                            where c.eissn = t.jcode
                            and c.managing_org is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update publisher name in citation object
        // using pissn code
        public void obtain_publisher_names_using_pissn()
        {
            string sql_string = @"with t as (
                            select e.pissn as jcode, p.publisher as pubname
                            from context_ctx.pub_pids e inner join context_ctx.publishers p
                            on e.pub_id = p.id
                        )
                        update sd.citation_objects c
                        set managing_org = t.pubname
                        from t
                        where c.pissn = t.jcode
                        and c.managing_org is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update publisher name in citation object
        // using journal title, for remainer
        // (but are journal titles unique - probably not...)
        public void obtain_publisher_names_using_journal_names()
        {
            string sql_string = @"with t as (
                            select e.journal_name as jname, p.publisher as pubname
                            from context_ctx.pub_journals e inner join context_ctx.publishers p
                            on e.pub_id = p.id
                        )
                        update sd.citation_objects c
                        set managing_org = t.pubname
                        from t
                        where lower(trim(c.journal_title)) = lower(trim(t.jname))
                        and c.managing_org is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update the org ids using the publisher name
        // against the organisations table(default name)
        public void update_publisher_ids_using_default_names()
        {

            string sql_string = @"with t as (
                            select id as orgid, default_name as orgname
                            from context_ctx.organisations
                        )
                        update sd.citation_objects c
                        set managing_org_id = t.orgid
                        from t
                        where lower(trim(c.managing_org)) = lower(trim(t.orgname))
                        and c.managing_org_id is null";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update the org ids using the publisher name
        // against the org_other_names table (other name)

        public void update_publisher_ids_using_other_names()
        {
            string sql_string = @"with t as (
                            select org_id as orgid, other_name as orgname
                            from context_ctx.org_other_names
                       )
                       update sd.citation_objects c
                       set managing_org_id = t.orgid
                       from t
                       where lower(trim(c.managing_org)) = lower(trim(t.orgname))
                       and c.managing_org_id is null";

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
                            set managing_org = g.default_name ||
                            case when g.display_suffix is not null and trim(g.display_suffix) <> '' 
                            then ' (' || g.display_suffix || ')'
                            else '' end
                            from context_ctx.organisations g
                            where c.managing_org_id = g.id;";

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
                            set identifier_org_id = c.managing_org_id
                            from sd.citation_objects c
                            where sd.object_identifiers.sd_oid = c.sd_oid
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
            string sql_string = @"update sd.object_identifiers i
                        set identifier_org = g.default_name ||
                        case when g.display_suffix is not null and trim(g.display_suffix) <> '' 
                             then ' (' || g.display_suffix || ')'
                        else '' end
                        from context_ctx.organisations g
                        where i.identifier_org_id = g.id
                        and i.identifier_type_id = 34
                        and i.identifier_org_id is not null;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void store_unmatched_publisher_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.orgs_to_match where source_id = "
            + source_id.ToString() + @" and source_table = 'citation_objects';
            insert into context_ctx.orgs_to_match (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'citation_objects', managing_org, count(managing_org) 
            from sd.citation_objects 
            where managing_org_id is null 
            group by managing_org; ";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        */

        public void transfer_citation_objects_to_data_objects()
        {
            string sql_string = @"insert into sd.data_objects
                        (sd_oid, sd_sid, 
                         display_title, version, doi, doi_status_id, publication_year,
                         object_class_id, object_class, object_type_id, object_type, 
                         managing_org_id, managing_org, lang_code, access_type_id, access_type,
                         access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
                         add_study_topics, datetime_of_data_fetch)
                        SELECT 
                         sd_oid, sd_sid, 
                         display_title, version, doi, doi_status_id, publication_year,
                         object_class_id, object_class, object_type_id, object_type, 
                         managing_org_id, managing_org, lang_code, access_type_id, access_type,
                         access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
                         add_study_topics, datetime_of_data_fetch
                        FROM sd.citation_objects;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void store_bank_links_in_pp_schema()
        {
            string sql_string = @"DROP TABLE IF EXISTS pp.bank_links;
                         CREATE TABLE pp.bank_links as
                         SELECT 
                         nlm.id as source_id, db.id_in_db as sd_sid, db.sd_oid as pmid
                         from sd.object_db_links db
                         inner join context_ctx.nlm_databanks nlm
                         on db.db_name = nlm.nlm_abbrev
                         where bank_type = 'Trial registry';";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void combine_distinct_study_pubmed_links()
        {
            string sql_string = @"DROP TABLE IF EXISTS pp.total_pubmed_links;
                        CREATE TABLE pp.total_pubmed_links as
                        SELECT source_id, sd_sid, pmid
                        FROM pp.bank_links
                        UNION
                        SELECT source_id, sd_sid, pmid
                        FROM pp.pmids_by_source_total;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }

}
