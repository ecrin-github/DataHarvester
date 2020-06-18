using PostgreSQLCopyHelper;


namespace DataHarvester.pubmed
{
    public class CopyHelpers
    {
        // defines the various copy helpers required.
        // Each copy helper maps fields in the receiving table with class elements
        // and is then used by a call to the Postgres bulk store routine
        // to transfer the data to he database.
        // see https://github.com/PostgreSQLCopyHelper/PostgreSQLCopyHelper for details

        //public PostgreSQLCopyHelper<PMIDRecord> pmid_copyhelper =
        //    new PostgreSQLCopyHelper<PMIDRecord>("pp", "pmid_list")
        //        .MapInteger("pmid", x => x.pmid);

        /*
        public PostgreSQLCopyHelper<Contributor> contributor_copyhelper =
            new PostgreSQLCopyHelper<Contributor>("sd", "object_contributors")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("person_id", x => x.person_id)
                .MapInteger("contributor_type_id", x => x.contributor_type_id)
                .MapVarchar("contributor_type", x => x.contributor_type)
                .MapVarchar("family_name", x => x.family_name)
                .MapVarchar("given_name", x => x.given_name)
                .MapVarchar("suffix", x => x.suffix)
                .MapVarchar("initials", x => x.initials)
                .MapVarchar("collective_name", x => x.collective_name);


        public PostgreSQLCopyHelper<Person_Identifier> persid_copyhelper =
            new PostgreSQLCopyHelper<Person_Identifier>("sd", "people_identifiers")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("person_id", x => x.person_id)
                .MapVarchar("identifier", x => x.identifier)
                .MapVarchar("identifier_source", x => x.identifier_source);


        public PostgreSQLCopyHelper<Person_Affiliation> persaff_copyhelper =
            new PostgreSQLCopyHelper<Person_Affiliation>("sd", "people_affiliations")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("person_id", x => x.person_id)
                .MapVarchar("affiliation", x => x.affiliation)
                .MapVarchar("affil_identifier", x => x.affil_identifier)
                .MapVarchar("affil_ident_source", x => x.affil_ident_source);


        public PostgreSQLCopyHelper<ObjectDate> object_date_copyhelper =
            new PostgreSQLCopyHelper<ObjectDate>("sd", "object_dates")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("date_type_id", x => x.date_type_id)
                .MapVarchar("date_type", x => x.date_type)
                .MapVarchar("date_as_string", x => x.date_as_string)
                .MapBoolean("date_is_range", x => x.date_is_range)
                .MapBoolean("date_is_partial", x => x.date_is_partial)
                .MapInteger("start_year", x => x.start_year)
                .MapInteger("start_month", x => x.start_month)
                .MapInteger("start_day", x => x.start_day)
                .MapInteger("end_year", x => x.end_year)
                .MapInteger("end_month", x => x.end_month)
                .MapInteger("end_day", x => x.end_day);


        public PostgreSQLCopyHelper<ObjectTitle> object_title_copyhelper =
            new PostgreSQLCopyHelper<ObjectTitle>("sd", "object_titles")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("title_type_id", x => x.title_type_id)
                .MapVarchar("title_type", x => x.title_type)
                .MapVarchar("title_text", x => x.title_text)
                .MapBoolean("is_default", x => x.is_default)
                .MapVarchar("lang_code", x => x.lang_code)
                .MapInteger("lang_status_id", x => x.lang_status_id)
                .MapBoolean("contains_html", x => x.contains_html)
                .MapVarchar("comments", x => x.comments);


        public PostgreSQLCopyHelper<Identifier> identifier_copyhelper =
            new PostgreSQLCopyHelper<Identifier>("sd", "object_identifiers")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("identifier_type_id", x => x.identifier_type_id)
                .MapVarchar("identifier_type", x => x.identifier_type)
                .MapVarchar("identifier_value", x => x.identifier_value)
                .MapInteger("identifier_org_id", x => x.identifier_org_id)
                .MapVarchar("identifier_org", x => x.identifier_org)
                .MapVarchar("date_applied", x => x.date_applied);


        public PostgreSQLCopyHelper<Instance> instance_copyhelper =
            new PostgreSQLCopyHelper<Instance>("sd", "object_instances")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("repository_org_id", x => x.repository_org_id)
                .MapVarchar("repository_org", x => x.repository_org)
                .MapInteger("instance_type_id", x => x.instance_type_id)
                .MapVarchar("instance_type", x => x.instance_type)
                .MapVarchar("url", x => x.url)
                .MapBoolean("url_direct_access", x => x.url_direct_access)
                .MapDate("url_last_checked", x => x.url_last_checked)
                .MapInteger("resource_type_id", x => x.resource_type_id)
                .MapVarchar("resource_type", x => x.resource_type);


        public PostgreSQLCopyHelper<Description> description_copyhelper =
            new PostgreSQLCopyHelper<Description>("sd", "object_descriptions")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("description_type_id", x => x.description_type_id)
                .MapVarchar("description_type", x => x.description_type)
                .MapVarchar("label", x => x.label)
                .MapVarchar("description_text", x => x.description_text)
                .MapVarchar("lang_code", x => x.lang_code)
                .MapBoolean("contains_html", x => x.contains_html);


        public PostgreSQLCopyHelper<DB_Accession_Number> db_acc_number_copyhelper =
            new PostgreSQLCopyHelper<DB_Accession_Number>("sd", "object_links")
                .MapInteger("sd_id", x => x.sd_id)
                .MapInteger("bank_id", x => x.bank_id)
                .MapVarchar("bank_name", x => x.bank_name)
                .MapVarchar("accession_number", x => x.accession_number);


        public PostgreSQLCopyHelper<Publication_Type> pub_type_copyhelper =
            new PostgreSQLCopyHelper<Publication_Type>("sd", "object_public_types")
                .MapInteger("sd_id", x => x.sd_id)
                .MapVarchar("type_name", x => x.type_name);


        public PostgreSQLCopyHelper<CommentCorrection> comment_correction_copyhelper =
            new PostgreSQLCopyHelper<CommentCorrection>("sd", "object_corrections")
                .MapInteger("sd_id", x => x.sd_id)
                .MapVarchar("ref_type", x => x.ref_type)
                .MapVarchar("ref_source", x => x.ref_source)
                .MapVarchar("pmid", x => x.pmid)
                .MapVarchar("pmid_version", x => x.pmid_version)
                .MapVarchar("note", x => x.note);


        public PostgreSQLCopyHelper<Topic> keyword_copyhelper =
            new PostgreSQLCopyHelper<Topic>("sd", "object_topics")
                .MapInteger("sd_id", x => x.sd_id)
                .MapVarchar("topic", x => x.topic)
                .MapInteger("topic_type_id", x => x.topic_type_id)
                .MapVarchar("topic_type", x => x.topic_type)
                .MapInteger("ct_scheme_id", x => x.ct_scheme_id)
                .MapVarchar("ct_scheme", x => x.ct_scheme)
                .MapVarchar("ct_scheme_code", x => x.ct_scheme_code)
                .MapVarchar("where_found", x => x.where_found);


        public PostgreSQLCopyHelper<ObjectLanguage> object_language_copyhelper =
            new PostgreSQLCopyHelper<ObjectLanguage>("sd", "object_languages")
                .MapInteger("sd_id", x => x.sd_id)
                .MapVarchar("lang_code", x => x.lang_code);
        */
    }
}
