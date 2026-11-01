using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5564: a range scan whose leaf read faults after the scan has started
/// yielding must fail the whole scan. It must never complete with fewer rows
/// than the tree holds.
/// <para>
/// The incident was an auth <c>ListGroups</c> that returned success without some
/// of the 230 groups in <c>sys-membership-groups</c>, while that tree's
/// shard-root to leaf <c>GetEntriesAsync</c> reads were timing out. Group
/// listings feed access administration, so a listing that silently omits rows
/// is a security defect as well as a correctness one. Every caller of a range
/// read - the membership directory, the Data facade's range reads, a view
/// rebuild - shares this scan stack. These fixtures pin the invariant at the
/// shared seam, <see cref="LatticeExtensions.ScanEntriesAsync"/> and
/// <see cref="LatticeExtensions.ScanKeysAsync"/> over a real multi-shard tree:
/// a scan either yields every key or throws.
/// </para>
/// <para>
/// The fault is injected as a <see cref="TimeoutException"/> - the shape an
/// Orleans response timeout takes at the shard root - on a leaf read that
/// carries a continuation (<c>afterExclusive</c>). That is a page after a
/// shard's first, so the fault lands mid-scan, after rows have already been
/// yielded to the caller. A page size of two forces every shard through
/// several pages.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ScanLeafReadFaultCompletenessIntegrationTests
{
    private const int KeyCount = 24;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        LeafReadFaultGate.Disarm();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void Disarm() => LeafReadFaultGate.Disarm();

    private async Task<(ILattice Tree, List<string> Keys)> CreatePopulatedTreeAsync(string prefix)
    {
        var treeName = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 2 });

        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var keys = new List<string>(KeyCount);
        for (var i = 0; i < KeyCount; i++)
        {
            var key = $"group-{i:D3}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes($"v{i}"));
            keys.Add(key);
        }

        // Precondition: an unfaulted scan sees every key, so any shortfall below
        // is the fault's doing and not the fixture's.
        var baseline = new List<string>();
        await foreach (var entry in tree.ScanEntriesAsync())
        {
            baseline.Add(entry.Key);
        }

        Assert.That(baseline, Is.EquivalentTo(keys),
            "PRECONDITION: an unfaulted scan must yield every written key.");

        // The baseline's leaf reads stay registered on the shard root, and a
        // later read with identical arguments against a leaf that has not
        // changed since is served from them without reaching the leaf (#2786),
        // so the fault filter below would never see it. Rewriting every key
        // moves each leaf's revision, which forces the faulted scan's reads to
        // be issued afresh.
        for (var i = 0; i < KeyCount; i++)
        {
            await tree.SetAsync(keys[i], Encoding.UTF8.GetBytes($"v{i}"));
        }

        return (tree, keys);
    }

    [TestCase(LeafReadFaultMode.Always)]
    [TestCase(LeafReadFaultMode.Once)]
    public async Task An_entry_scan_whose_leaf_read_faults_mid_scan_throws_rather_than_completing_short(LeafReadFaultMode mode)
    {
        var (tree, keys) = await CreatePopulatedTreeAsync("scan-leaf-fault-entries");

        var yielded = new List<string>();
        Exception? fault = null;
        LeafReadFaultGate.Arm(mode);
        try
        {
            await foreach (var entry in tree.ScanEntriesAsync())
            {
                yielded.Add(entry.Key);
            }
        }
        catch (Exception ex)
        {
            fault = ex;
        }
        finally
        {
            LeafReadFaultGate.Disarm();
        }

        AssertCompleteOrFaulted(mode, keys, yielded, fault, await DescribeShortScanAsync(tree, keys, yielded, fault));
    }

    [TestCase(LeafReadFaultMode.Always)]
    [TestCase(LeafReadFaultMode.Once)]
    public async Task A_key_scan_whose_leaf_read_faults_mid_scan_throws_rather_than_completing_short(LeafReadFaultMode mode)
    {
        var (tree, keys) = await CreatePopulatedTreeAsync("scan-leaf-fault-keys");

        var yielded = new List<string>();
        Exception? fault = null;
        LeafReadFaultGate.Arm(mode);
        try
        {
            await foreach (var key in tree.ScanKeysAsync())
            {
                yielded.Add(key);
            }
        }
        catch (Exception ex)
        {
            fault = ex;
        }
        finally
        {
            LeafReadFaultGate.Disarm();
        }

        AssertCompleteOrFaulted(mode, keys, yielded, fault, await DescribeShortScanAsync(tree, keys, yielded, fault));
    }

    // Issue #5607: a scan that completes short says nothing about whether the
    // rows it omitted are gone or were merely skipped. Point-read each omitted
    // key (a miss is lost data, a hit is a scan fault) and rescan with the
    // fault disarmed, so a failure separates the two without a rerun. Collected
    // only when the scan completed short; never fails the test itself.
    private static async Task<string> DescribeShortScanAsync(
        ILattice tree, List<string> keys, List<string> yielded, Exception? fault)
    {
        if (fault is not null || yielded.SequenceEqual(keys.Order(StringComparer.Ordinal), StringComparer.Ordinal))
        {
            return string.Empty;
        }

        var report = new StringBuilder();
        try
        {
            var missing = keys.Except(yielded, StringComparer.Ordinal).ToList();
            var reads = new List<string>(missing.Count);
            foreach (var key in missing)
            {
                reads.Add($"{key}={(await tree.GetAsync(key) is null ? "MISS" : "hit")}");
            }

            report.Append($" Point reads of the missing keys (a MISS is lost data, a hit is a scan fault): {string.Join(", ", reads)}.");

            var rescan = new List<string>();
            await foreach (var key in tree.ScanKeysAsync())
            {
                rescan.Add(key);
            }

            report.Append($" An unfaulted rescan yielded {rescan.Count} of {keys.Count}.");
        }
        catch (Exception ex)
        {
            report.Append($" The miss report could not be collected: {ex.GetType().Name}: {ex.Message}");
        }

        return report.ToString();
    }

    private static void AssertCompleteOrFaulted(
        LeafReadFaultMode mode, List<string> keys, List<string> yielded, Exception? fault, string missReport)
    {
        Assert.That(LeafReadFaultGate.FiredCount, Is.GreaterThan(0),
            "PRECONDITION: the injected leaf-read fault must actually have fired, or the scan was never "
            + "exercised against a faulting leaf and the assertions below are vacuous.");

        if (fault is not null)
        {
            Assert.That(yielded, Is.Not.Empty,
                "PRECONDITION: the fault must land mid-scan, after rows were yielded. A fault before the "
                + "first row exercises only scan setup, not the truncation the incident showed.");
            return;
        }

        Assert.That(mode, Is.Not.EqualTo(LeafReadFaultMode.Always),
            "every continuation leaf read faulted, yet the scan completed without throwing. It can only "
            + $"have done that by treating a faulted read as the end of a shard: it yielded {yielded.Count} "
            + $"of {keys.Count} keys.{missReport}");

        // A scan that recovered from a transient fault and completed must have
        // yielded every key, each once, in order.
        Assert.That(yielded, Is.EqualTo(keys.Order(StringComparer.Ordinal).ToList()),
            "the scan completed without throwing but did not yield every key the tree holds, exactly once "
            + $"and in order: yielded {yielded.Count} of {keys.Count}, missing "
            + $"[{string.Join(", ", keys.Except(yielded, StringComparer.Ordinal).Take(8))}].{missReport}");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.KeysPageSize = 2);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, LeafReadFaultFilter>();
        }
    }

    /// <summary>How the armed gate faults continuation leaf reads.</summary>
    public enum LeafReadFaultMode
    {
        /// <summary>Every continuation leaf read faults while armed.</summary>
        Always,

        /// <summary>Only the first continuation leaf read faults.</summary>
        Once,
    }

    /// <summary>
    /// Control state for the injected fault. Static because the TestingHost silo
    /// runs in-process; disarmed after every test.
    /// </summary>
    private static class LeafReadFaultGate
    {
        private static int _mode = -1;
        private static int _fired;

        internal static int FiredCount => Volatile.Read(ref _fired);

        internal static void Arm(LeafReadFaultMode mode)
        {
            Volatile.Write(ref _fired, 0);
            Volatile.Write(ref _mode, (int)mode);
        }

        internal static void Disarm() => Volatile.Write(ref _mode, -1);

        internal static bool ShouldFault()
        {
            var mode = Volatile.Read(ref _mode);
            if (mode < 0)
            {
                return false;
            }

            var fired = Interlocked.Increment(ref _fired);
            return mode == (int)LeafReadFaultMode.Always || fired == 1;
        }
    }

    /// <summary>
    /// Faults a leaf range read that resumes after a continuation token, the
    /// read a shard root issues for every page after a shard's first.
    /// </summary>
    private sealed class LeafReadFaultFilter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (context.InterfaceMethod?.DeclaringType == typeof(IBPlusLeafGrain)
                && context.InterfaceMethod.Name is nameof(IBPlusLeafGrain.GetEntriesAsync) or nameof(IBPlusLeafGrain.GetKeysAsync)
                && context.Request.GetArgument(2) is string
                && LeafReadFaultGate.ShouldFault())
            {
                throw new TimeoutException("injected: a continuation leaf read timed out mid-scan (#5564)");
            }

            return context.Invoke();
        }
    }
}
