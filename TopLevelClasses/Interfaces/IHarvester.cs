using System.Threading.Tasks;

namespace DataHarvester
{
    interface IHarvester
    {
        Task<int> RunAsync(Options opts);
    }
}