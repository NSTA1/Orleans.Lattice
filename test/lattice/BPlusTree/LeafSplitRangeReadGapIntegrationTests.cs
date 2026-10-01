using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #3918, against a real <see cref="ILattice"/> tree on an in-process
/// cluster: a <b>completed</b> range read that comes up short of records the
/// store holds and hands back, unchanged, through a point read.
/// <para>
/// <b>The seam.</b> A donor leaf persists <c>SplitInFlight</c>, <c>SplitKey</c>
/// and the sibling pointer in one block at the <i>start</i> of a division, long
/// before a single row moves. From that instant its four range-read entry
/// points - <c>CountAsync</c>, <c>GetStatsAsync</c>, <c>GetKeysAsync</c> and
/// <c>GetEntriesAsync</c> - clip their scan at <c>SplitKey</c>, on the stated
/// reasoning that the right half "already lives on the new sibling". For the
/// prefix of the division that runs before the first <c>MergeEntriesAsync</c>
/// that is simply untrue: the rows are still on the donor and the sibling holds
/// none, so those keys are reported by <b>neither</b> leaf. The point and
/// batched read paths carry no such clip, so they keep returning the very rows
/// the range paths have stopped reporting.
/// </para>
/// <para>
/// In a healthy division that disagreement lasts milliseconds. A division left
/// interrupted - by a crash, a deactivation, or a WAL replay refused under the
/// permit saturation of #3905 - makes it permanent, and it is silent: no
/// exception, no empty result to be suspicious of, just a scan that is quietly
/// missing rows. That is the exact symptom
/// <c>test/lattice.vector/Fakes/ReadGapVectorIndexStore.cs</c> models for
/// #3915, reproduced here against the real core rather than a fake.
/// </para>
/// <para>
/// <b>These are discriminators, not guards.</b> Each one fails on the
/// pre-#3918 source and passes after it. They deliberately assert the tree-level
/// invariant from the issue's own acceptance - "a core read either returns every
/// record the store holds in range or throws; it never completes short" - rather
/// than any internal field, so they cannot be satisfied by relabelling state.
/// </para>
/// <para>
/// The interruption instrument is the one
/// <see cref="LeafReclaimUnderInterruptedSplitIntegrationTests"/> introduced for
/// #2160: failing the old-next leaf's back-pointer fixup stops the division
/// after the splice and before any row moves, while targeting a third grain so
/// neither the donor nor the sibling is blocked or faulted.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafSplitRangeReadGapIntegrationTests
{
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
        SplitInterruptGate.Disarm();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void Disarm() => SplitInterruptGate.Disarm();

    private IBPlusLeafGrain Leaf(GrainId id) => _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id);

    private async Task<(ILattice Router, IShardRootGrain Shard)> CreateTreeAsync(string treeName)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });

        return (_cluster.GrainFactory.GetGrain<ILattice>(treeName),
                _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0"));
    }

    /// <summary>The state of an interrupted division, as the fixture established it.</summary>
    private sealed record Window(GrainId Donor, GrainId Sibling, List<string> Keys);

    /// <summary>
    /// Drives a real division of the chain head and stops it inside the
    /// seed-to-merge window, asserting every precondition the gap depends on
    /// rather than assuming it. A fixture that silently missed the window would
    /// make the assertions downstream of it vacuous.
    /// </summary>
    private async Task<Window> InterruptASplitInsideTheWindowAsync(ILattice router, IShardRootGrain shard)
    {
        var keys = new List<string>();
        for (var i = 0; i < 40; i++)
        {
            var key = $"k{i:D3}";
            await router.SetAsync(key, Encoding.UTF8.GetBytes($"v{i}"));
            keys.Add(key);
        }

        var donor = (await shard.GetLeftmostLeafIdAsync())!.Value;
        var oldNext = (await Leaf(donor).GetNextSiblingAsync())!.Value;

        SplitInterruptGate.Arm(oldNext);

        // '!' sorts below every key already present, so these all route to the
        // head and overflow it into a genuine division.
        for (var i = 0; i < 20 && !SplitInterruptGate.Fired; i++)
        {
            var key = $"!{i:D3}";
            keys.Add(key);
            try
            {
                await router.SetAsync(key, Encoding.UTF8.GetBytes($"split-{i}"));
            }
            catch
            {
                // The injected interruption surfaces here. Which write trips it
                // does not matter; that the division stopped inside the window
                // does, and that is asserted below rather than inferred.
                break;
            }
        }

        SplitInterruptGate.Disarm();

        Assert.That(SplitInterruptGate.Fired, Is.True,
            "PRECONDITION: the interruption must actually have fired, or no division was stopped inside "
            + "the window and everything below is vacuous.");

        var donorProbe = await Leaf(donor).GetReclaimProbeAsync();

        Assert.That(donorProbe.SplitTargetSiblingId, Is.Not.Null,
            "PRECONDITION: the donor must be left mid-division - that is the state in which its range "
            + "reads clip at SplitKey.");

        var sibling = donorProbe.SplitTargetSiblingId!.Value;
        var siblingProbe = await Leaf(sibling).GetReclaimProbeAsync();

        Assert.That(siblingProbe.LiveRowCount, Is.Zero,
            "PRECONDITION, and the whole point: the window is BEFORE the first MergeEntriesAsync, so the "
            + "sibling holds no rows at all. The right-half rows are therefore still on the donor, and "
            + "the donor's clip is hiding rows that exist nowhere else.");

        return new Window(donor, sibling, keys);
    }

    /// <summary>
    /// Reads every live key through the range path, which is what a consumer
    /// rebuilding a projection from the tree walks.
    /// </summary>
    private static async Task<HashSet<string>> ScanKeysAsync(ILattice router)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var entry in router.ScanEntriesAsync(null, null))
        {
            seen.Add(entry.Key);
        }

        return seen;
    }

    [Test]
    public async Task A_range_scan_returns_every_key_a_point_read_still_answers()
    {
        var treeName = $"split-read-gap-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);
        var window = await InterruptASplitInsideTheWindowAsync(router, shard);

        var scanned = await ScanKeysAsync(router);

        // The discriminator. A key the point read still answers for is a record
        // the store holds; a scan that omits it has completed short.
        var heldButUnscanned = new List<string>();
        foreach (var key in window.Keys)
        {
            if (scanned.Contains(key)) continue;
            if (await router.GetAsync(key) is not null)
            {
                heldButUnscanned.Add(key);
            }
        }

        Assert.That(heldButUnscanned, Is.Empty,
            "a completed range scan omitted keys the store still holds and still hands back through "
            + "GetAsync. The scan threw nothing and looked healthy, so no consumer can detect this: "
            + $"missing {heldButUnscanned.Count} key(s), e.g. "
            + $"[{string.Join(", ", heldButUnscanned.Take(8))}].");
    }

    [Test]
    public async Task A_batched_read_and_a_range_scan_agree_on_what_the_store_holds()
    {
        var treeName = $"split-read-gap-many-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);
        var window = await InterruptASplitInsideTheWindowAsync(router, shard);

        var scanned = await ScanKeysAsync(router);
        var batched = await router.GetManyAsync([.. window.Keys]);

        // This is the precise shape ReadGapVectorIndexStore models for #3915:
        // one read path omits a record another returns. A consumer that uses
        // both - as DurableVectorIndex does, loading chunks by prefix scan and
        // confirming by batched read - sees the store contradict itself.
        var batchedOnly = batched.Keys.Where(k => !scanned.Contains(k)).Order(StringComparer.Ordinal).ToList();

        Assert.That(batchedOnly, Is.Empty,
            "GetManyAsync returned records that a full range scan did not, so the two read paths "
            + "disagree about what the store holds. Neither call failed: "
            + $"[{string.Join(", ", batchedOnly.Take(8))}].");
    }

    [Test]
    public async Task A_count_matches_the_number_of_keys_the_scan_yields()
    {
        var treeName = $"split-read-gap-count-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);
        var window = await InterruptASplitInsideTheWindowAsync(router, shard);

        var scanned = await ScanKeysAsync(router);
        var counted = await router.CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(counted, Is.EqualTo(scanned.Count),
                "CountAsync and the range scan must fold the same rows; they share the clip under test, "
                + "so this holds before and after the fix and is a guard on the fix not splitting them.");

            Assert.That(counted, Is.EqualTo(window.Keys.Count),
                $"the tree holds {window.Keys.Count} acknowledged keys, so a count that completed without "
                + $"throwing must report all of them. It reported {counted}.");
        });
    }

    /// <summary>
    /// The division must still be completable afterwards. A fix that exposed the
    /// right-half rows by abandoning the division would trade a read gap for a
    /// chain defect, so the resume is driven and the tree re-checked.
    /// </summary>
    [Test]
    public async Task The_division_still_completes_and_the_tree_is_whole_afterwards()
    {
        var treeName = $"split-read-gap-resume-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);
        var window = await InterruptASplitInsideTheWindowAsync(router, shard);

        // A write to the donor's range resumes the division it opened on.
        await router.SetAsync("!999", Encoding.UTF8.GetBytes("resume"));
        window.Keys.Add("!999");

        var donorProbe = await Leaf(window.Donor).GetReclaimProbeAsync();
        Assert.That(donorProbe.SplitTargetSiblingId, Is.Null,
            "the division must have completed on resume, clearing the split marker.");

        var scanned = await ScanKeysAsync(router);

        Assert.That(scanned, Is.SupersetOf(window.Keys),
            "once the division completes every acknowledged key must be reachable by a range scan again.");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.TombstoneGracePeriod = TimeSpan.Zero);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, SplitInterruptFilter>();
        }
    }

    /// <summary>
    /// Control state for the injected interruption. Static because the
    /// TestingHost silo runs in-process; disarmed after every test so it cannot
    /// leak into the resume the last test drives.
    /// </summary>
    private static class SplitInterruptGate
    {
        private static GrainId? _target;
        private static int _fired;

        internal static bool Fired => Volatile.Read(ref _fired) == 1;

        internal static void Arm(GrainId target)
        {
            Volatile.Write(ref _fired, 0);
            _target = target;
        }

        internal static void Disarm() => _target = null;

        /// <summary>
        /// Throws for EVERY matching call while armed, not just the first. The
        /// shard root retries a failed leaf dispatch and <c>SetAsync</c> opens
        /// by resuming any division already in flight, so a one-shot
        /// interruption is repaired by the very retry it provoked and the test
        /// would regain control with the window already gone.
        /// </summary>
        internal static bool ShouldInterrupt(GrainId grainId)
        {
            if (_target != grainId) return false;
            Volatile.Write(ref _fired, 1);
            return true;
        }
    }

    /// <summary>
    /// Fails the armed leaf's back-pointer fixup, which
    /// <c>CompleteSplitAsync</c> awaits after seeding the new sibling and
    /// before moving any row into it.
    /// </summary>
    private sealed class SplitInterruptFilter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (context.InterfaceMethod?.DeclaringType == typeof(IBPlusLeafGrain)
                && context.InterfaceMethod?.Name == nameof(IBPlusLeafGrain.SetPrevSiblingAsync)
                && SplitInterruptGate.ShouldInterrupt(context.TargetContext.GrainId))
            {
                throw new InvalidOperationException(
                    "injected: interrupting the division inside its seed-to-merge window");
            }

            return context.Invoke();
        }
    }
}
