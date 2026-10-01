using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #2160, against a real <see cref="ILattice"/> tree on an in-process
/// cluster: a division interrupted between the sibling splice and the row
/// merge, with the real empty-leaf reclaim pass then walked over it.
/// <para>
/// The window is the interval inside <c>CompleteSplitAsync</c> between
/// <c>InitializeSiblingAsync</c> seeding the new sibling's key range and the
/// first <c>MergeEntriesAsync</c> moving rows into it. In that interval the
/// sibling is spliced into the chain, declares a real range, holds zero rows,
/// and is not yet routed by any separator - so it looks exactly like a
/// reclaimable empty leaf to every piece of evidence it carries itself.
/// </para>
/// <para>
/// <b>How the window is driven, and why this is the honest instrument.</b>
/// <c>CompleteSplitAsync</c> starts the old-next leaf's back-pointer fixup
/// before seeding the sibling and <c>await</c>s it after, so failing that one
/// call stops the division precisely inside the window - after the splice,
/// before any row moves. It targets a third grain, so neither the donor nor
/// the sibling is blocked or faulted, and what is left behind is exactly what
/// a crash, a deactivation or a storage fault in that interval leaves: durable
/// state, reached through a real write to a real tree rather than assembled by
/// hand. The injection is an incoming call filter, the same deterministic
/// mechanism <c>LeafChainTilingIntegrationTests</c> uses to drive a declined
/// fold without having to win a race for real.
/// </para>
/// <para>
/// <b>Read most of these as GUARDS, not as the discriminator.</b> The
/// declination inside <c>TryUnlinkSuccessorAsync</c> already refuses to unlink
/// a division's target, so "the reclaim declines" holds both before and after
/// this change. The discriminator for what was still broken - that the pass
/// takes the retirement LATCH on the sibling before it asks the donor anything
/// - is <c>ShardRootGrainLeafReclaimResilienceTests.SplitSuccessor</c>, which
/// asserts <c>TryBeginRetirementAsync</c> is never called.
/// <see cref="The_retirement_latch_is_lethal_to_the_divisions_merge"/> below is
/// the premise that makes that assertion matter, demonstrated rather than
/// argued.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafReclaimUnderInterruptedSplitIntegrationTests
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
    /// seed-to-merge window.
    /// <para>
    /// Every precondition the window depends on is asserted here rather than
    /// assumed. A fixture that silently missed the window would make every
    /// assertion downstream of it vacuous - and vacuous is precisely how this
    /// defect survived its first fix, so the issue asked for the window to be
    /// a precondition rather than an observation.
    /// </para>
    /// </summary>
    private async Task<Window> InterruptASplitInsideTheWindowAsync(ILattice router, IShardRootGrain shard)
    {
        // Grow past one leaf so the head has a successor to act as oldNext -
        // without one the division has no back-pointer fixup to interrupt.
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

        // Overflow the head so it genuinely divides. '!' sorts below every key
        // already present, so these all route to the head.
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
            "PRECONDITION: the interruption must actually have fired. If it did not, no division was "
            + "stopped inside the window and everything below is vacuous.");

        var donorProbe = await Leaf(donor).GetReclaimProbeAsync();

        Assert.That(donorProbe.SplitTargetSiblingId, Is.Not.Null,
            "PRECONDITION: the donor must be left mid-division, which is the only state in which a "
            + "freshly seeded sibling is reachable by the reclaim walk at all.");

        var sibling = donorProbe.SplitTargetSiblingId!.Value;

        Assert.That(donorProbe.NextSibling, Is.EqualTo(sibling),
            "PRECONDITION: the division persists NextSibling in the same block as the split marker, so "
            + "the sibling is already chain-reachable - that is what lets the walk step onto it.");

        var siblingProbe = await Leaf(sibling).GetReclaimProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(siblingProbe.LiveRowCount, Is.Zero,
                "PRECONDITION: the window is BEFORE the first MergeEntriesAsync, so the sibling holds no "
                + "rows. A non-zero count means the merge already ran, the retirement latch could never "
                + "have been taken, and this fixture would be passing for the wrong reason.");
            Assert.That(siblingProbe.LowKeyInclusive, Is.Not.Null,
                "PRECONDITION: InitializeSiblingAsync has already seeded the sibling's declared range.");
            Assert.That(siblingProbe.HasBlockingState, Is.False,
                "PRECONDITION, and the whole difficulty: the sibling's OWN state says it is perfectly safe "
                + "to reclaim. It is a fresh grain - SplitState.Unsplit, no seal, no saga - so no probe of "
                + "it can ever report otherwise. The evidence lives on the donor.");
        });

        return new Window(donor, sibling, keys);
    }

    [Test]
    public async Task A_reclaim_pass_over_an_interrupted_split_declines_and_leaves_the_division_completable()
    {
        var treeName = $"reclaim-interrupted-split-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);
        var window = await InterruptASplitInsideTheWindowAsync(router, shard);

        var donorRangeBefore = await Leaf(window.Donor).GetKeyRangeAsync();
        var siblingRangeBefore = await Leaf(window.Sibling).GetKeyRangeAsync();

        // The real pass, over the real chain, with a real division in flight.
        var reclaimed = await shard.ReclaimEmptyLeavesAsync(16);

        Assert.That(reclaimed, Is.Zero,
            "the division's own target must not be folded, however empty it looks");

        Assert.That(await Leaf(window.Donor).GetNextSiblingAsync(), Is.EqualTo(window.Sibling),
            "the chain must still run through the division's target");

        var donorRangeAfter = await Leaf(window.Donor).GetKeyRangeAsync();
        Assert.That(donorRangeAfter.HighKeyExclusive, Is.EqualTo(donorRangeBefore.HighKeyExclusive),
            "no widen may leak through a declined fold: a donor claiming a range it does not route to "
            + "loses every write into it on the next projection rebuild");

        var siblingRangeAfter = await Leaf(window.Sibling).GetKeyRangeAsync();
        Assert.That(siblingRangeAfter.LowKeyInclusive, Is.EqualTo(siblingRangeBefore.LowKeyInclusive),
            "and the sibling must still own the range the division seeded it with");

        // Declining rather than failing loudly is only correct if the division
        // is still completable afterwards. A write to the donor resumes it
        // through the ordinary recovery path.
        await router.SetAsync("!999", Encoding.UTF8.GetBytes("resume"));
        window.Keys.Add("!999");

        foreach (var key in window.Keys)
        {
            Assert.That(await router.GetAsync(key), Is.Not.Null,
                $"every key written before the interruption must still be readable after it; '{key}' is not");
        }
    }

    [Test]
    public async Task No_orphaned_leaf_survives_a_reclaim_pass_over_an_interrupted_split()
    {
        // The end state stated in the same terms the production audit reports
        // in: a leaf in the sibling chain that no descent reaches. That is what
        // this defect manufactures, and it is the live deployment's signature.
        var treeName = $"reclaim-interrupted-orphan-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);
        var window = await InterruptASplitInsideTheWindowAsync(router, shard);

        await shard.ReclaimEmptyLeavesAsync(16);

        await router.SetAsync("!999", Encoding.UTF8.GetBytes("resume"));
        window.Keys.Add("!999");

        var survey = await shard.SurveyOrphanedLeavesAsync(null);

        Assert.That(survey.Findings, Is.Empty,
            "once the division completes, no leaf may be left in the chain that descent cannot reach");

        foreach (var key in window.Keys)
        {
            Assert.That(await router.GetAsync(key), Is.Not.Null,
                $"and every key must be readable through the router; '{key}' is not");
        }
    }

    [Test]
    public async Task The_retirement_latch_is_lethal_to_the_divisions_merge()
    {
        // WHY the fold has to decline BEFORE it latches, demonstrated rather
        // than argued. This characterises MergeEntriesAsync, so it holds both
        // before and after this change - that is the point. It is the premise
        // the pre-latch declination rests on, and the issue asked for it as a
        // precondition assertion rather than an observation, so that a fixture
        // which never reaches the merge cannot be mistaken for evidence that
        // the merge is safe.
        var treeName = $"reclaim-latch-lethal-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);
        var window = await InterruptASplitInsideTheWindowAsync(router, shard);

        // Exactly what ShardRootGrain.TryReclaimLeafAsync does to a candidate
        // before it asks the donor anything.
        Assert.That(await Leaf(window.Sibling).TryBeginRetirementAsync(), Is.True,
            "PRECONDITION: a freshly seeded division target latches like any other empty leaf - there is "
            + "nothing on it to refuse with, which is the entire problem");

        try
        {
            Assert.ThrowsAsync<LeafRetiredException>(
                async () => await Leaf(window.Sibling).MergeEntriesAsync(
                    new Dictionary<string, LwwValue<byte[]>>
                    {
                        ["!500"] = LwwValue<byte[]>.Create(
                            Encoding.UTF8.GetBytes("row"), HybridLogicalClock.Tick(default)),
                    }),
                "While the latch is set the division's merge is refused. CompleteSplitAsync has no "
                + "try/catch at that call site, and the shard root's retirement backoff covers the "
                + "write-dispatch path only, so this escapes the division and its whole tail - the "
                + "straggler sweep, the re-narrow to the split key, the SplitInFlight clear, and the "
                + "parent separator the caller publishes afterwards - never runs. The sibling is left "
                + "spliced into the chain and unreachable by descent. THAT is why the fold must decline "
                + "before it latches, not after.");
        }
        finally
        {
            await Leaf(window.Sibling).AbandonRetirementAsync();
        }
    }

    [Test]
    public async Task An_ordinary_empty_leaf_is_still_folded_when_no_division_is_in_flight()
    {
        // The falsifier for every declination above. A guard that quietly
        // refused every fold would leave empty-leaf reclaim inert while all of
        // those assertions stayed green, because none of them can tell
        // "declined for the right reason" from "never folds anything".
        var treeName = $"reclaim-ordinary-fold-{Guid.NewGuid():N}";
        var (router, shard) = await CreateTreeAsync(treeName);

        for (var i = 0; i < 120; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        await router.DeleteRangeAsync("k030", "k090");

        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        Assert.That(reclaimed, Is.GreaterThan(0),
            "an emptied leaf with no division in flight must still be folded out of the chain - the new "
            + "declination must not disable reclaim");
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
    /// leak into the recovery the tests then drive.
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
        /// Throws for EVERY matching call while armed, not just the first, and
        /// that is load-bearing rather than incidental. The shard root retries
        /// a failed leaf dispatch, and <c>SetAsync</c> opens by resuming any
        /// division already in flight - so a one-shot interruption is repaired
        /// by the very retry it provoked, inside the same call, and the test
        /// regains control with the division already complete and the window
        /// gone. Holding the interruption for the whole armed period is what
        /// leaves the division genuinely stopped; disarming afterwards is what
        /// lets the test then drive the resume deliberately.
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
