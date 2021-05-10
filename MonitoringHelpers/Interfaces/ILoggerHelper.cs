namespace DataHarvester
{
    public interface ILoggerHelper
    {
        void LogCommandLineParameters(Options opts);
        void Logheader(string header_text);
        void LogTableStatistics(ISource s, string schema);
    }
}