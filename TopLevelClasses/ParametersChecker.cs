using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Serilog;
using CommandLine;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace DataHarvester
{
    internal class ParametersChecker : IParametersChecker
    {
        private readonly ILogger _logger;
        private Credentials _credentials;
        MonitorDataLayer _mon_repo;

        public ParametersChecker(ILogger logger, IConfiguration settings)
        {
            _logger = logger;

            // get the credentials object set up
            _credentials = new Credentials();
            _credentials.GetCredentials(settings);
        }

        public Credentials Credentials  => _credentials;
        public MonitorDataLayer Mon_repo => _mon_repo;


        public Options ObtainParsedArguments(string[] args)
        {
            // Parse command line arguments and return true only if no errors.
            // Otherwise log errors and return false.

            var parsedArguments = Parser.Default.ParseArguments<Options>(args);
            if (parsedArguments.Tag.ToString() == "NotParsed")
            {
                HandleParseError(((NotParsed<Options>)parsedArguments).Errors, _logger);
                return null;
            }
            else
            {
                return ((Parsed<Options>)parsedArguments).Value;
            }
        }


        public bool ValidArgumentValues(Options opts)
        {

            // Parse command line arguments and return true if values are valid.
            // Otherwise log errors and return false.

            try
            {
                int harvest_type_id = opts.harvest_type_id;
                if (harvest_type_id != 1 && harvest_type_id != 2 && harvest_type_id != 3)
                {
                    throw new Exception("The t (harvest type) parameter is not one of the allowed values - 1,2 or 3");
                }

                // set up the monitoring data repo
                _mon_repo = new MonitorDataLayer(_logger, _credentials);

                foreach (int source_id in opts.source_ids)
                {
                    if (!_mon_repo.SourceIdPresent(source_id))
                    {
                        throw new ArgumentException("Source argument " + source_id.ToString() +
                                                    " does not correspond to a known source");
                    }
                }

                // Got this far - the program can run!
                return true;
            }

            catch (Exception e)  
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger.Information("Harvester application aborted");
                _logger.Information("Closing Log\n");
                return false;
            }

        }


        private void HandleParseError(IEnumerable<Error> errs, ILogger logger)
        {
            // log the errors
            logger.Error("Error in the command line arguments - they could not be parsed");
            int n = 0;
            foreach (Error e in errs)
            {
                n++;
                logger.Error("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
                if (e.GetType().Name == "UnknownOptionError")
                {
                    logger.Error("Error {n}: Unknown option was {UnknownOption}", n.ToString(), ((UnknownOptionError)e).Token);
                }
                if (e.GetType().Name == "MissingRequiredOptionError")
                {
                    logger.Error("Error {n}: Missing option was {MissingOption}", n.ToString(), ((MissingRequiredOptionError)e).NameInfo.NameText);
                }
                if (e.GetType().Name == "BadFormatConversionError")
                {
                    logger.Error("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), ((BadFormatConversionError)e).NameInfo.NameText);
                }
            }
            logger.Information("Harvester application aborted");
            _logger.Information("Closing Log\n");
        }

    }



    internal class Options
    {
        // Lists the command line arguments and options

        [Option('s', "source_ids", Required = true, Separator = ',', HelpText = "Comma separated list of Integer ids of data sources.")]
        public IEnumerable<int> source_ids { get; set; }

        [Option('t', "harvest_type_id", Required = true, HelpText = "Integer representing type of harvest (1 = full, i.e. all available files, 2 = only files downloaded since last import, 3 = test data only.")]
        public int harvest_type_id { get; set; }

        [Option('G', "organisation_update_only", Required = false, HelpText = "If present does not recreate sd tables - only updates organisation ids")]
        public bool org_update_only { get; set; }

    }

}





