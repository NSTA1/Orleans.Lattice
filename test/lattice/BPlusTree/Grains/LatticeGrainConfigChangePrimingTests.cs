using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2918: the <c>config</c> taxonomy on
/// <c>orleans_lattice_config_changed_total</c> must be readable at zero.
/// <para>
/// <b>What was wrong.</b> The counter carries a bounded two-member domain and
/// the only writes were the two increments, each in a different setter. A tree
/// whose publish-events override had never been touched published no
/// <c>config="publish_events"</c> series at all - indistinguishable from a build
/// in which that setter no longer records, and indistinguishable again from one
/// where the counter is unwired. A flat absence therefore could not be read as
/// "no such change has been made", which is the only reading the instrument
/// exists to support.
/// </para>
/// <para>
/// <b>Why activation is the seam and the setters are not.</b> The two arms are
/// armed from two different methods. Priming inside each setter would leave each
/// arm's zero reachable only on the path that also arms it, which is the same
/// defect one layer in: <c>history_retention</c> would still be absent on a tree
/// that had only ever had its publish-events flag set. The lifecycle hook is
/// reachable for every tree that activates, so it primes both arms independent
/// of which setter, if either, is ever called.
/// </para>
/// <para>
/// <b>And on the issue's stated cause.</b> #2918 filed this as unprimable
/// because <c>LatticeMetrics</c> is a static class with no silo-startup hook.
/// The instrument is emitted from a grain, and the grain has a lifecycle; no new
/// hook was needed.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeGrainConfigChangePrimingTests
{
    private const string TreeId = "config-priming-tree";

    /// <summary>
    /// The config arms this counter declares, taken from the two emission sites.
    /// </summary>
    private static readonly string[] ConfigArms = ["publish_events", "history_retention"];

    private static LatticeGrain CreateGrain(string treeId)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        return new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver,
            Substitute.For<IServiceProvider>(), NullLogger<LatticeGrain>.Instance);
    }

    private static (MeterListener Listener, Dictionary<string, long> Totals) ListenForConfigArms(string treeId)
    {
        var totals = new Dictionary<string, long>(StringComparer.Ordinal);
        var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ConfigChanged,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? config = null;
                var onThisTree = false;

                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagConfig && tag.Value is string arm)
                    {
                        config = arm;
                    }
                    else if (tag.Key == LatticeMetrics.TagTree && tag.Value is string tree)
                    {
                        onThisTree = string.Equals(tree, treeId, StringComparison.Ordinal);
                    }
                }

                if (!onThisTree || config is null)
                {
                    return;
                }

                lock (totals)
                {
                    totals[config] = totals.TryGetValue(config, out var running) ? running + value : value;
                }
            }));

        return (listener, totals);
    }

    /// <summary>
    /// Activation alone, with neither setter called, must publish both config
    /// arms at zero.
    /// <para>
    /// Reverting the prime in <c>LatticeGrain.OnActivateAsync</c> reddens this
    /// and nothing else: no measurement is published, so both arms are absent
    /// rather than zero.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Activation_alone_primes_both_config_arms_at_zero()
    {
        var (listener, totals) = ListenForConfigArms(TreeId);

        await CreateGrain(TreeId).OnActivateAsync(CancellationToken.None);

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(totals.Keys, Is.EquivalentTo(ConfigArms),
                "activation must publish both declared config arms, so that an absent series means the build "
                + "does not carry the instrument and nothing else (issue #2918).");

            foreach (var arm in ConfigArms)
            {
                Assert.That(totals.GetValueOrDefault(arm, -1), Is.Zero,
                    $"the '{arm}' arm must be primed at zero, not incremented: activation changes no "
                    + "configuration, so a non-zero total would mean this assertion passes for the wrong reason.");
            }
        });
    }

    /// <summary>
    /// The positive control: a real config change must be reported as a one on
    /// its own arm by this same listener, with the other arm still at zero.
    /// <para>
    /// Without it, the test above is satisfied equally by a harness that
    /// observes nothing, because a primed zero and an unobserved measurement
    /// produce the same assertion. Driving the setter through the same listener
    /// is what makes the zeros above measured zeros.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_real_config_change_is_reported_as_one_on_its_arm_by_this_same_harness()
    {
        const string ControlTree = "config-priming-control-tree";
        const string Changed = "history_retention";

        var (listener, totals) = ListenForConfigArms(ControlTree);
        var grain = CreateGrain(ControlTree);

        await grain.OnActivateAsync(CancellationToken.None);
        await grain.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, TimeSpan.FromHours(1));

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(Changed, Is.AnyOf(ConfigArms),
                "the arm this control expects must be one the grain emits, or it asserts against a literal "
                + "the source no longer uses");

            Assert.That(totals.GetValueOrDefault(Changed, -1), Is.EqualTo(1),
                "this harness must observe a real config change as a one. If it reports zero here, the zeros "
                + "the priming test asserts are 'the harness saw nothing' and that test is vacuous.");

            Assert.That(totals.GetValueOrDefault("publish_events", -1), Is.Zero,
                "the arm that was not changed must still read zero, which is what shows the harness "
                + "attributes a measurement to the arm that produced it rather than to both.");
        });
    }

    /// <summary>
    /// A system tree must not be primed.
    /// <para>
    /// Both setters reject a reserved tree id outright, so no system tree can
    /// ever arm this counter. Priming one would publish a measured zero for a
    /// measurement that cannot be taken - asserting the tree is capable of a
    /// configuration change it will always refuse - which is the same category
    /// of misleading claim the priming exists to remove, pointed the other way.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_system_tree_is_not_primed_because_it_can_never_arm_the_counter()
    {
        var systemTree = LatticeConstants.SystemTreePrefix + "config-priming";

        var (listener, totals) = ListenForConfigArms(systemTree);

        await CreateGrain(systemTree).OnActivateAsync(CancellationToken.None);

        listener.Dispose();

        Assert.That(totals, Is.Empty,
            "a system tree rejects both config setters, so priming it would assert a measured zero for a "
            + "measurement that can never be taken.");
    }
}
