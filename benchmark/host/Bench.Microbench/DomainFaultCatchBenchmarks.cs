using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the catch-filter cost used by the scan and coordinator refusal audit.
/// This is not an end-to-end scan throughput measurement.
/// </summary>
[MemoryDiagnoser]
public class DomainFaultCatchBenchmarks
{
    private Exception? _fault;

    /// <summary>The successful, runtime-timeout and domain-timeout paths.</summary>
    [Params("none", "runtime", "domain")]
    public string Fault { get; set; } = "none";

    /// <summary>Constructs faults outside the measured operation.</summary>
    [GlobalSetup]
    public void Setup() => _fault = Fault switch
    {
        "runtime" => new TimeoutException("response deadline"),
        "domain" => new LatticeTransactionOutcomeUnavailableException("registry unavailable"),
        _ => null,
    };

    /// <summary>The previous BCL-shaped handler.</summary>
    [Benchmark(Baseline = true)]
    public bool BroadCatch()
    {
        try
        {
            Read();
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>The filtered handler with a caller-owned domain remedy.</summary>
    [Benchmark]
    public bool FilteredCatch()
    {
        try
        {
            try
            {
                Read();
                return true;
            }
            catch (TimeoutException fault) when (fault is not ILatticeDomainFault)
            {
                return false;
            }
        }
        catch (LatticeTransactionOutcomeUnavailableException)
        {
            return false;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Read()
    {
        if (_fault is { } fault) throw fault;
    }
}
