using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Real-grain coverage of issue #4525: a WAL placement move must not lose an
/// acknowledged append when the fenced source activation is lost between the
/// coordinator's final quiesce and the placement flip.
/// <para>
/// The interleaving runs inside the target provider's verification read (see
/// <see cref="HookedWalStorageProvider"/>), which the coordinator issues after
/// its last <c>QuiesceForMoveAsync</c> and before the compare-and-swap that flips
/// the pin. There the test deactivates the source WAL shard (the activation is
/// lost) and appends through whatever activation Orleans brings up next. The
/// invariant every test asserts is the one the issue breaks: every append the
/// WAL acknowledged is readable from the live placement at the offset it was
/// acknowledged with, and no offset is acknowledged twice.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalMoveDurableFenceIntegrationTests
{
    private const string Secondary = "secondary";

    private WalMoveFenceClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new WalMoveFenceClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TearDown]
    public void TearDown() => WalMoveFenceProviders.Secondary.Disarm();

    private ILatticeAdmin Admin =>
        _fixture.Cluster.Client.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);

    private IWalShardGrain Wal(string physicalTreeId) =>
        _fixture.Cluster.Client.GetGrain<IWalShardGrain>($"{physicalTreeId}/0");

    private static WalRecord Record(string physicalTreeId, string key) => new()
    {
        TreeId = physicalTreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = Encoding.UTF8.GetBytes(key),
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
    };

    /// <summary>Appends <paramref name="key"/> and records the acknowledged offset.</summary>
    private static async Task AppendAckedAsync(IWalShardGrain wal, string physicalTreeId, string key, List<(long Offset, string Key)> acked)
    {
        var offset = await wal.AppendAsync(Record(physicalTreeId, key), CancellationToken.None);
        lock (acked)
        {
            acked.Add((offset, key));
        }
    }

    /// <summary>
    /// Asserts that no offset was acknowledged twice and that every acknowledged
    /// append reads back, from the partition's live placement, at its offset.
    /// </summary>
    private static async Task AssertEveryAckedAppendIsReadableAsync(IWalShardGrain wal, List<(long Offset, string Key)> acked)
    {
        var page = await wal.ReadAsync(0, 10_000, CancellationToken.None);
        var live = page.Entries.ToDictionary(e => e.Sequence, e => e.Entry.Key);
        var reissued = acked.GroupBy(a => a.Offset).Where(g => g.Count() > 1)
            .Select(g => $"offset {g.Key} acknowledged to [{string.Join(", ", g.Select(a => a.Key))}]")
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(reissued, Is.Empty, "an acknowledged WAL offset was reissued to a second append");
            foreach (var (offset, key) in acked)
            {
                Assert.That(
                    live.TryGetValue(offset, out var found) ? found : "<missing>",
                    Is.EqualTo(key),
                    $"the append acknowledged at offset {offset} must read back from the live placement");
            }
        });
    }

    /// <summary>
    /// The interleaving named in issue #4525. After the final quiesce the source
    /// activation is lost, a fresh activation of the source is asked to append,
    /// and the move then flips. Before the fix the fresh activation came up
    /// unfenced, acknowledged the append at the source's next offset, and the flip
    /// stranded it on the orphaned source while the target reissued the offset.
    /// The durable fence makes the fresh activation refuse the append.
    /// </summary>
    [Test]
    public async Task A_source_activation_lost_after_the_final_quiesce_cannot_acknowledge_an_append_the_flip_strands()
    {
        var treeId = $"fence-lost-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var wal = Wal(physical);
        var acked = new List<(long Offset, string Key)>();
        for (var i = 0; i < 5; i++)
        {
            await AppendAckedAsync(wal, physical, $"pre-{i}", acked);
        }
        var quiescedHighest = acked.Max(a => a.Offset);

        Exception? duringMoveRefusal = null;
        WalMoveFenceProviders.Secondary.ArmOnceAtHighest(quiescedHighest, async () =>
        {
            // The fenced source activation is lost after the coordinator's last
            // quiesce. The next call activates the source afresh.
            await wal.DeactivateForMoveAsync(CancellationToken.None);
            try
            {
                await AppendAckedAsync(wal, physical, "during-move", acked);
            }
            catch (LatticeWalQuiescingException ex)
            {
                duringMoveRefusal = ex;
            }
        });

        var receipt = await Admin.ExecuteWalMoveAsync(treeId, 0, Secondary);
        Assert.That(WalMoveFenceProviders.Secondary.HookRuns, Is.EqualTo(1), "the interleaving hook must have run");
        Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));

        await AppendAckedAsync(wal, physical, "post-move", acked);

        await AssertEveryAckedAppendIsReadableAsync(wal, acked);
        Assert.That(
            duringMoveRefusal,
            Is.Not.Null,
            "a fresh activation of a source that is fenced for a move must come up fenced and refuse the append");
    }

    /// <summary>
    /// A fence the coordinator held past its lease is released by the next
    /// activation of the source, which then serves appends again. The flip must
    /// see that the fence it raised is gone and refuse to cut over, because the
    /// source may hold acknowledged appends the copy never saw. Before the fix
    /// the flip went ahead and stranded them.
    /// </summary>
    [Test]
    public async Task A_flip_after_the_source_fence_lapsed_and_served_an_append_is_refused()
    {
        var treeId = $"fence-lapsed-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var wal = Wal(physical);
        var acked = new List<(long Offset, string Key)>();
        for (var i = 0; i < 5; i++)
        {
            await AppendAckedAsync(wal, physical, $"pre-{i}", acked);
        }
        var quiescedHighest = acked.Max(a => a.Offset);
        var lease = TimeSpan.FromSeconds(2);

        WalMoveFenceProviders.Secondary.ArmOnceAtHighest(quiescedHighest, async () =>
        {
            // The coordinator stalls past its lease; the source activation is lost
            // and the next one serves an append once the fence has lapsed.
            await Task.Delay(lease + TimeSpan.FromSeconds(1));
            await wal.DeactivateForMoveAsync(CancellationToken.None);
            await AppendAckedAsync(wal, physical, "after-lapse", acked);
        });

        var placementBefore = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(
            async () => await Admin.ExecuteWalMoveAsync(treeId, 0, Secondary, new WalMoveOptions { QuiesceLease = lease, VerifyAfterCopy = true }),
            Throws.InstanceOf<InvalidOperationException>(),
            "the flip must be refused once the fence it raised has lapsed and been released");
        Assert.That(WalMoveFenceProviders.Secondary.HookRuns, Is.EqualTo(1), "the interleaving hook must have run");

        var placementAfter = await Admin.GetWalPlacementAsync(treeId);
        Assert.Multiple(() =>
        {
            Assert.That(placementAfter.Version, Is.EqualTo(placementBefore.Version), "a refused flip must not change the placement");
            Assert.That(placementAfter.Partitions[0].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        });

        await AppendAckedAsync(wal, physical, "after-refusal", acked);
        await AssertEveryAckedAppendIsReadableAsync(wal, acked);
    }

    /// <summary>
    /// The liveness half: a durable fence must never wedge a partition whose move
    /// coordinator died. The fence is raised exactly as a coordinator raises it and
    /// then abandoned. Activations of the source refuse appends while its lease
    /// holds; once it lapses the next activation releases it and serves again.
    /// </summary>
    [Test]
    public async Task A_fence_abandoned_by_a_dead_coordinator_refuses_appends_until_its_lease_lapses_then_is_released()
    {
        var treeId = $"fence-dead-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var wal = Wal(physical);
        var acked = new List<(long Offset, string Key)>();
        await AppendAckedAsync(wal, physical, "before", acked);

        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physical);
        await registry.RaiseWalMoveFencesAsync(physical, pin.Version, [0], "dead-coordinator", TimeSpan.FromSeconds(2), renew: false);

        // The activation that predates the fence is lost; its replacement reads
        // the durable fence and comes up fenced.
        await wal.DeactivateForMoveAsync(CancellationToken.None);
        Assert.That(
            async () => await wal.AppendAsync(Record(physical, "while-fenced"), CancellationToken.None),
            Throws.InstanceOf<LatticeWalQuiescingException>(),
            "a re-activated source must refuse appends while a durable move fence is held");

        await TestPoll.UntilAsync(
            async () =>
            {
                try
                {
                    await AppendAckedAsync(wal, physical, "after-lapse", acked);
                    return true;
                }
                catch (LatticeWalQuiescingException)
                {
                    return false;
                }
            },
            "the source to serve appends again once the abandoned fence lapsed",
            TimeSpan.FromSeconds(20),
            TimeSpan.FromMilliseconds(100));

        var after = await registry.GetWalPlacementAsync(physical);
        Assert.That(after.ResolveFence(0), Is.Null, "the activation that served again released the lapsed fence");
        await AssertEveryAckedAppendIsReadableAsync(wal, acked);
    }
}
