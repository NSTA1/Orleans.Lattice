using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the per-tree decision-sequence fan-out of the replication
/// cross-tree decision stamper. A replicated cross-tree commit issues one
/// sequence per participating tree, and later confirms one per tree, each on
/// that tree's own <c>ICrossTreeDecisionSequenceGrain</c>. The stamper awaited
/// those grain calls one after another, so a commit spanning P trees paid P
/// sequential round trips (each a durable state write) to issue and P more to
/// confirm. The grains are independent and both calls are idempotent per
/// operation id, so the shipped stamper issues them together and the critical
/// path falls from P round trips to one.
/// <para>
/// The baseline arms cannot call the shipped stamper because the shipped
/// stamper is the concurrent one, so both shells mirror its body exactly - the
/// frontier registration first, the same per-participant calls, the same result
/// dictionary - and differ only in the dispatch shape.
/// </para>
/// <para>
/// <b>Read the hop model.</b> A store that completes synchronously prices a
/// round trip at zero, so the serial chain and the concurrent wave cost the same
/// against it. <see cref="HopModel.Yield"/> models a grain call's asynchronous
/// completion with one yield, orders of magnitude cheaper than a real hop, so it
/// prices only the dispatch overhead the wave adds. <see cref="HopModel.Delay1Ms"/>
/// models a remote round trip with <c>Task.Delay(1)</c>, which the OS timer
/// rounds up (about 15.6 ms on Windows, about 1 ms on Linux), so its absolute
/// times are platform-dependent and its <b>ratio</b> is the evidence: the serial
/// lane pays 2P hops on the critical path and the concurrent lane pays two. The
/// call count is unchanged in every lane; what the change removes is the
/// serialisation of those calls.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=decisionstamp</c> (or
/// <c>--suite decisionstamp</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class CrossTreeDecisionStampBenchmarks
{
    private const string OperationId = "op-0001";

    private string[] _participants = null!;
    private SequenceStore _store = null!;

    /// <summary>
    /// Trees a cross-tree commit spans. Two is the smallest cross-tree write;
    /// eight is a wide multi-tree transaction.
    /// </summary>
    [Params(2, 8)]
    public int ParticipantCount { get; set; }

    /// <summary>How each simulated grain call completes; see the class summary.</summary>
    [Params(HopModel.Yield, HopModel.Delay1Ms)]
    public HopModel Hop { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _participants = new string[ParticipantCount];
        for (var i = 0; i < ParticipantCount; i++)
        {
            _participants[i] = "tree-" + i.ToString("D2", CultureInfo.InvariantCulture);
        }

        _store = new SequenceStore(Hop);

        var serial = IssueAndConfirm_Serial_Async().GetAwaiter().GetResult();
        var concurrent = IssueAndConfirm_Concurrent_Async().GetAwaiter().GetResult();
        if (serial != concurrent)
        {
            throw new InvalidOperationException(
                $"Decision-stamp lanes disagree: serial={serial}, concurrent={concurrent}.");
        }
    }

    [Benchmark(Baseline = true, Description = "issue+confirm: P serial grain calls each")]
    public async Task<long> IssueAndConfirm_Serial_Async()
    {
        await _store.RegisterTreesAsync(_participants);
        var sequences = new Dictionary<string, long>(_participants.Length, StringComparer.Ordinal);
        foreach (var tree in _participants)
        {
            sequences[tree] = await _store.IssueAsync(tree, OperationId);
        }

        foreach (var tree in _participants)
        {
            await _store.ConfirmAsync(tree, OperationId);
        }

        return Sum(sequences);
    }

    [Benchmark(Description = "issue+confirm: P concurrent grain calls each")]
    public async Task<long> IssueAndConfirm_Concurrent_Async()
    {
        await _store.RegisterTreesAsync(_participants);
        var issues = new Task<long>[_participants.Length];
        for (var i = 0; i < _participants.Length; i++)
        {
            issues[i] = _store.IssueAsync(_participants[i], OperationId);
        }

        var issued = await Task.WhenAll(issues);
        var sequences = new Dictionary<string, long>(_participants.Length, StringComparer.Ordinal);
        for (var i = 0; i < _participants.Length; i++)
        {
            sequences[_participants[i]] = issued[i];
        }

        var confirms = new Task[_participants.Length];
        for (var i = 0; i < _participants.Length; i++)
        {
            confirms[i] = _store.ConfirmAsync(_participants[i], OperationId);
        }

        await Task.WhenAll(confirms);
        return Sum(sequences);
    }

    private static long Sum(Dictionary<string, long> sequences)
    {
        var total = 0L;
        foreach (var value in sequences.Values)
        {
            total += value;
        }

        return total;
    }

    /// <summary>How a simulated grain call completes.</summary>
    public enum HopModel
    {
        /// <summary>One yield: prices dispatch overhead only.</summary>
        Yield,

        /// <summary>One <c>Task.Delay(1)</c>: prices a remote round trip at the OS timer floor.</summary>
        Delay1Ms,
    }

    /// <summary>
    /// Stands in for the frontier and per-tree sequence grains: each call
    /// completes asynchronously per the hop model, the shape an Orleans grain
    /// call has, and returns a deterministic per-tree sequence so the lanes can
    /// be checked to agree.
    /// </summary>
    private sealed class SequenceStore(HopModel hop)
    {
        public Task RegisterTreesAsync(IReadOnlyCollection<string> trees) => HopAsync();

        public async Task<long> IssueAsync(string tree, string operationId)
        {
            await HopAsync();
            return tree.Length + operationId.Length;
        }

        public Task ConfirmAsync(string tree, string operationId) => HopAsync();

        private async Task HopAsync()
        {
            if (hop == HopModel.Delay1Ms)
            {
                await Task.Delay(1);
            }
            else
            {
                await Task.Yield();
            }
        }
    }
}
