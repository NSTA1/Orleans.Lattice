using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5564: a scan-page retry that attaches to a leaf read still in flight
/// must not hand back rows older than its own start.
/// <para>
/// The shard root coalesces a stall-guarded page fill's leaf read with an
/// identical read an abandoned walk already issued, so a retry does not queue a
/// duplicate behind it (issue 2585). The leaf can have executed that read before
/// the retry began, with only the response still in transit. A write
/// acknowledged in that window is ordered after the read, so serving the read's
/// rows to a scan that started after the acknowledgement completes the scan
/// successfully without the write - a silent partial read.
/// </para>
/// <para>
/// The fixture makes the window deterministic. An outgoing call filter holds the
/// response of the first leaf read of the tree's first leaf after the leaf has
/// answered. The first scan's page stalls on it and is abandoned, freeing the
/// shard; a write is then acknowledged on that leaf; a second scan starts, its
/// page reaches the same leaf read while the response is still held, and only
/// then is the response released.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ScanPageLeafReadJoinRecencyIntegrationTests
{
    private static readonly TimeSpan StallCeiling = TimeSpan.FromSeconds(2);

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
        ResponseHold.Disarm();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void Disarm() => ResponseHold.Disarm();

    [Test]
    public async Task A_scan_started_after_an_acknowledged_write_includes_it_when_its_page_attaches_to_an_older_in_flight_leaf_read()
    {
        var (tree, keys) = await CreateTreeAsync("join-recency-write");
        await StallFirstScanOnHeldLeafReadAsync(tree);

        await tree.SetAsync("k00a", Encoding.UTF8.GetBytes("k00a"));
        keys.Add("k00a");
        keys.Sort(StringComparer.Ordinal);

        var second = CollectAsync(tree);
        await ResponseHold.WaitForPageAfterArmAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        ResponseHold.Release();

        Assert.That(await second.WaitAsync(TimeSpan.FromSeconds(30)), Is.EqualTo(keys),
            "a scan that began after k00a was acknowledged completed without it: its page served the rows "
            + "of a leaf read the leaf had answered before the write.");
    }

    [Test]
    public async Task A_retry_with_no_intervening_write_still_serves_the_in_flight_leaf_read_without_reading_the_leaf_again()
    {
        var (tree, keys) = await CreateTreeAsync("join-recency-quiet");
        await StallFirstScanOnHeldLeafReadAsync(tree);

        var readsBefore = ResponseHold.HeldLeafReads;
        var second = CollectAsync(tree);
        await ResponseHold.WaitForPageAfterArmAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        ResponseHold.Release();

        Assert.Multiple(async () =>
        {
            Assert.That(await second.WaitAsync(TimeSpan.FromSeconds(30)), Is.EqualTo(keys));
            Assert.That(ResponseHold.HeldLeafReads, Is.EqualTo(readsBefore),
                "the retry read the held leaf again instead of attaching to the read already in flight, "
                + "which is the duplicate the coalescing exists to suppress (issue 2585).");
        });
    }

    private async Task<(ILattice Tree, List<string> Keys)> CreateTreeAsync(string prefix)
    {
        var treeName = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });

        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var keys = new List<string>();
        for (var i = 0; i < 12; i++)
        {
            var key = $"k{i:D2}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(key));
            keys.Add(key);
        }

        ResponseHold.Arm(treeName, heldKey: "k00");
        return (tree, keys);
    }

    // The first scan's page fill reads the leaf holding k00, whose response is
    // held, so the ceiling abandons the page with no leaf completed and the scan
    // fails. The shard is then free while the read stays in flight.
    private static async Task StallFirstScanOnHeldLeafReadAsync(ILattice tree)
    {
        Assert.ThrowsAsync<ScanPageStalledException>(() => CollectAsync(tree, maxAttempts: 0),
            "PRECONDITION: the first scan must stall on the held leaf read, leaving it in flight.");
        Assert.That(ResponseHold.IsHolding, Is.True, "PRECONDITION: the leaf read must still be held.");
        await ResponseHold.ExpectNextPageAsync();
    }

    private static async Task<List<string>> CollectAsync(ILattice tree, int? maxAttempts = null)
    {
        var keys = new List<string>();
        await foreach (var entry in tree.ScanEntriesAsync(maxAttempts: maxAttempts))
        {
            keys.Add(entry.Key);
        }

        return keys;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.MaxScanPageStallDuration = StallCeiling;
                o.MaxScanPageDuration = TimeSpan.Zero;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<ResponseHoldFilter>();
        }
    }

    /// <summary>
    /// Holds the response of the first answered leaf read containing a chosen
    /// key, counts leaf reads that would return it, and signals page fills.
    /// Static because the TestingHost silo runs in-process.
    /// </summary>
    private static class ResponseHold
    {
        private static readonly object Gate = new();
        private static string? _tree;
        private static string? _heldKey;
        private static bool _holding;
        private static int _heldLeafReads;
        private static TaskCompletionSource _release = NewSignal();
        private static TaskCompletionSource _pageAfterArm = NewSignal();

        internal static bool IsHolding
        {
            get { lock (Gate) return _holding; }
        }

        internal static int HeldLeafReads => Volatile.Read(ref _heldLeafReads);

        internal static void Arm(string tree, string heldKey)
        {
            lock (Gate)
            {
                _tree = tree;
                _heldKey = heldKey;
                _holding = false;
                _heldLeafReads = 0;
                _release = NewSignal();
                _pageAfterArm = NewSignal();
            }
        }

        internal static void Disarm()
        {
            lock (Gate)
            {
                _tree = null;
                _release.TrySetResult();
            }
        }

        internal static void Release()
        {
            lock (Gate) _release.TrySetResult();
        }

        internal static Task ExpectNextPageAsync()
        {
            lock (Gate) _pageAfterArm = NewSignal();
            return Task.CompletedTask;
        }

        internal static Task WaitForPageAfterArmAsync()
        {
            Task signal;
            lock (Gate) signal = _pageAfterArm.Task;
            return signal.WaitAsync(TimeSpan.FromSeconds(10));
        }

        internal static void OnPage(string target)
        {
            lock (Gate)
            {
                if (_tree is not null && target.Contains(_tree, StringComparison.Ordinal))
                    _pageAfterArm.TrySetResult();
            }
        }

        internal static Task? OnLeafRead(List<KeyValuePair<string, byte[]>> rows)
        {
            lock (Gate)
            {
                if (_tree is null || _heldKey is null || !rows.Exists(r => r.Key == _heldKey))
                    return null;

                _heldLeafReads++;
                if (_holding)
                    return null;

                _holding = true;
                return _release.Task;
            }
        }

        private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class ResponseHoldFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.DeclaringType == typeof(IShardRootGrain)
                && context.InterfaceMethod.Name == nameof(IShardRootGrain.GetSortedEntriesBatchAsync))
            {
                ResponseHold.OnPage(context.TargetId.ToString());
            }

            await context.Invoke();

            if (context.InterfaceMethod?.DeclaringType == typeof(IBPlusLeafGrain)
                && context.InterfaceMethod.Name == nameof(IBPlusLeafGrain.GetEntriesAsync)
                && context.Result is List<KeyValuePair<string, byte[]>> rows
                && ResponseHold.OnLeafRead(rows) is { } hold)
            {
                await hold;
            }
        }
    }
}
