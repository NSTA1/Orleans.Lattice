using NUnit.Framework;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Proves what the same-silo grain-call path does to a
/// <c>Dictionary&lt;string, LwwValue&lt;byte[]&gt;&gt;</c> argument - the exact
/// parameter shape of <c>IBPlusLeafGrain.MergeEntriesAsync</c> - with and
/// without an immutability marker on the parameter (issue #2799).
/// <para>
/// The observable is reference identity of the payload array, which is
/// structural and deterministic. It is deliberately NOT a memory or timing
/// measurement: those are host-dependent and would flake.
/// </para>
/// <para>
/// The cluster is pinned to a SINGLE silo. That is not tidiness - with the
/// default two silos the two probe grains may be placed on different silos, the
/// call then travels the serialization path instead of the same-silo copy path,
/// and the marked arm reports a fresh array for a reason that has nothing to do
/// with the attribute under test. Single-silo is also the configuration in which
/// this copy is reachable on every single call, which is what makes it worth
/// removing.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class MergeEntriesArgumentCopyTests
{
    private TestCluster cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        cluster = new TestClusterBuilder(initialSilosCount: 1).Build();
        await cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await cluster.StopAllSilosAsync();
        await cluster.DisposeAsync();
    }

    /// <summary>
    /// The control arm, and the reason a green result on the marked arm means
    /// anything. An unmarked argument must be deep-copied, so the callee must
    /// NOT see the caller's array. If this ever passes trivially - if the probe
    /// reported "aliased" here too - the marked arm below would be measuring
    /// nothing.
    /// </summary>
    [Test]
    public async Task An_unmarked_batch_argument_is_deep_copied_on_the_same_silo_path()
    {
        var caller = cluster.GrainFactory.GetGrain<IArgumentCopyCallerGrain>("copied-arm");

        var aliased = await caller.CopiedArmAliasesCallerPayloadAsync();

        Assert.That(aliased, Is.False,
            "an unmarked grain-call argument must be deep-copied on the same-silo path. "
            + "If this fails, the probe cannot distinguish a copy from an alias and the "
            + "companion immutability test proves nothing.");
    }

    /// <summary>
    /// The arm under test: marking the parameter immutable suppresses the
    /// same-silo deep copy, so the callee receives the caller's own array.
    /// </summary>
    [Test]
    public async Task An_immutable_marked_batch_argument_is_not_deep_copied_on_the_same_silo_path()
    {
        var caller = cluster.GrainFactory.GetGrain<IArgumentCopyCallerGrain>("immutable-arm");

        var aliased = await caller.ImmutableArmAliasesCallerPayloadAsync();

        Assert.That(aliased, Is.True,
            "marking the parameter immutable must suppress the same-silo deep copy, so the "
            + "callee receives the caller's own payload array rather than a reallocated one.");
    }
}
