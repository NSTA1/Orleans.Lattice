using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Proves the behavioural half of issue #3180: a long-running whole-registry
/// enumeration must not head-of-line-block the registry singleton.
/// <para>
/// <c>LatticeRegistryGrain</c> is a process-wide singleton and
/// <c>GetAllTreeIdsAsync</c> fans out a whole-keyspace scan over the registry's
/// own backing system tree, descending into <c>LatticeGrain</c> and
/// <c>BPlusLeafGrain</c> activations. Every grain activation resolves its
/// options through <c>LatticeOptionsResolver</c>, which calls straight back
/// into this same singleton from five separate call sites. With no member
/// interleaving, the enumeration held the singleton's only turn for the length
/// of the scan and every unrelated option resolution queued behind it - which
/// on the live deployment showed up as a climbing
/// <c>NonReentrancyQueueSize</c>, timeouts all targeting this one grain, and a
/// WAL GC scheduler that completed zero enumerations while the WAL grew with no
/// reclamation at all.
/// </para>
/// <para>
/// Both tests park a real <c>GetAllTreeIdsAsync</c> call on a real Orleans
/// activation and assert a concurrent caller still completes. The parking is
/// done with an incoming grain-call filter that awaits a gate before
/// <see cref="IIncomingGrainCallContext.Invoke"/>, which is the only mechanism
/// that works here. The obvious alternative - gating the backing
/// <c>IGrainStorage</c>, as
/// <c>ShardRootGrainIsSplittingInterleaveTests</c> does - cannot discriminate,
/// because every registry read descends into the same <c>_lattice_trees</c>
/// leaf, so parking that leaf's read would hang the probe as well as the
/// blocker and both arms would fail regardless of the fix.
/// </para>
/// <para>
/// The two tests are complementary and pull in opposite directions, which is
/// the point. The reader test is the literal acceptance criterion: a point read
/// must overtake the parked enumeration, which it does because the point reads
/// carry <c>[AlwaysInterleave]</c>. The mutator test pins the boundary of that
/// fix: a mutator must <i>not</i> overtake it, because the enumeration is
/// deliberately left non-interleaving so that no registration can reshape the
/// backing tree under an in-flight scan cursor. Marking the enumeration too -
/// the first attempt at this fix - passes the first test and fails the second,
/// and in production silently drops ids from the scan result.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeRegistryEnumerationHeadOfLineTests
{
    private const string UnrelatedTree = "registry-headofline-unrelated-tree";
    private const string LateRegisteredTree = "registry-headofline-late-tree";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var registry = Registry();
        await registry.RegisterAsync(UnrelatedTree);

        // Warm the enumeration path once, ungated, so neither test below can be
        // measuring first-touch activation of the backing tree's grains.
        await registry.GetAllTreeIdsAsync(null).WaitAsync(TimeSpan.FromSeconds(60));
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        RegistryEnumerationGate.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ReleaseGate() => RegistryEnumerationGate.Reset();

    /// <summary>
    /// The acceptance criterion: a parked whole-registry enumeration must not
    /// prevent a concurrent read for an unrelated tree from completing. This is
    /// the shape of the production wedge - the WAL GC scheduler enumerating
    /// while every activation in the process resolves its options through the
    /// same singleton.
    /// </summary>
    [Test]
    public async Task Unrelated_read_completes_while_a_whole_registry_enumeration_is_parked()
    {
        var registry = Registry();
        RegistryEnumerationGate.Arm();

        Task<IReadOnlyList<string>>? blocker = null;
        try
        {
            blocker = registry.GetAllTreeIdsAsync(null);
            await RegistryEnumerationGate.Entered!.Task.WaitAsync(TimeSpan.FromSeconds(30));

            var probe = registry.GetEntryAsync(UnrelatedTree);
            var winner = await Task.WhenAny(probe, Task.Delay(TimeSpan.FromSeconds(10)));

            Assert.That(winner, Is.SameAs(probe),
                "GetEntryAsync for an unrelated tree must complete while a whole-registry enumeration is " +
                "in flight on the same singleton. It queued behind the parked enumeration instead, which " +
                "is the head-of-line block of issue #3180: in production this is where the response " +
                "timeout fires, for every activation in the process at once.");

            Assert.That(await probe, Is.Not.Null,
                "The interleaved read should have observed the registered entry, not a null from a " +
                "half-completed turn.");

            // ResolveAsync is the call LatticeOptionsResolver actually makes on
            // its hot path, so pin it too rather than inferring it from
            // GetEntryAsync.
            var resolve = registry.ResolveAsync(UnrelatedTree);
            var resolveWinner = await Task.WhenAny(resolve, Task.Delay(TimeSpan.FromSeconds(10)));

            Assert.That(resolveWinner, Is.SameAs(resolve),
                "ResolveAsync must also interleave - it is the registry call LatticeOptionsResolver makes " +
                "from five separate call sites during grain activation.");
            Assert.That(await resolve, Is.EqualTo(UnrelatedTree));
        }
        finally
        {
            RegistryEnumerationGate.Release();
            if (blocker is not null)
            {
                await blocker.WaitAsync(TimeSpan.FromSeconds(60));
            }
        }
    }

    /// <summary>
    /// The counterpart property, and the one whose loss regressed the tag-index
    /// reconcile: a mutator must stay excluded for the length of the
    /// enumeration. The scan is a multi-hop range traversal of the registry's
    /// own backing tree, and the system-tree scan path deliberately omits the
    /// topology re-probes that would otherwise re-enter this grain - so a
    /// registration admitted mid-scan can reshape the tree under the cursor and
    /// silently drop an unrelated, already-registered id.
    /// <para>
    /// A mutator carries no interleave attribute by design, so it is admitted
    /// only when the activation holds no turn, which makes it a direct probe of
    /// whether the parked enumeration is still holding one. Here it must lose
    /// the race. Marking <c>GetAllTreeIdsAsync</c> <c>[AlwaysInterleave]</c>
    /// releases the turn and turns this test red, which is the behavioural half
    /// of the contract guard in
    /// <c>LatticeRegistryInterleaveContractTests</c>.
    /// </para>
    /// <para>
    /// This does not conflict with the head-of-line fix above: that is about
    /// option resolution overtaking the scan, and option resolution goes through
    /// the point reads, which do interleave.
    /// </para>
    /// </summary>
    [Test]
    public async Task Registration_is_excluded_while_a_whole_registry_enumeration_is_parked()
    {
        var registry = Registry();
        RegistryEnumerationGate.Arm();

        Task<IReadOnlyList<string>>? blocker = null;
        Task? register = null;
        try
        {
            blocker = registry.GetAllTreeIdsAsync(null);
            await RegistryEnumerationGate.Entered!.Task.WaitAsync(TimeSpan.FromSeconds(30));

            register = registry.RegisterAsync(LateRegisteredTree);
            var winner = await Task.WhenAny(register, Task.Delay(TimeSpan.FromSeconds(5)));

            Assert.That(winner, Is.Not.SameAs(register),
                "RegisterAsync completed while GetAllTreeIdsAsync was parked mid-scan, so the " +
                "enumeration is no longer holding the registry's turn. A mutator admitted during the " +
                "scan can reshape the backing tree under the cursor and drop an already-registered id " +
                "from the result, which is how marking the enumeration [AlwaysInterleave] broke the " +
                "tag-index reconcile trigger. Mark the point reads instead: those are what option " +
                "resolution calls, and marking them is what lets it overtake this scan.");
        }
        finally
        {
            RegistryEnumerationGate.Release();
            if (blocker is not null)
            {
                await blocker.WaitAsync(TimeSpan.FromSeconds(60));
            }
            if (register is not null)
            {
                await register.WaitAsync(TimeSpan.FromSeconds(60));
                await registry.UnregisterAsync(LateRegisteredTree);
            }
        }
    }

    private ILatticeRegistry Registry() =>
        _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, RegistryEnumerationGatingFilter>();
        }
    }

    /// <summary>
    /// Control state for the gate. Static because the TestingHost silo runs
    /// in-process, and disarmed after every test so it cannot leak.
    /// </summary>
    private static class RegistryEnumerationGate
    {
        internal static volatile TaskCompletionSource? Gate;
        internal static volatile TaskCompletionSource? Entered;

        internal static void Arm()
        {
            Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        internal static void Release() => Gate?.TrySetResult();

        internal static void Reset()
        {
            Gate?.TrySetResult();
            Gate = null;
            Entered = null;
        }
    }

    /// <summary>
    /// Parks <c>ILatticeRegistry.GetAllTreeIdsAsync</c> before
    /// <see cref="IIncomingGrainCallContext.Invoke"/> while the gate is armed.
    /// An incoming grain-call filter runs as part of the request's execution on
    /// the activation, so awaiting here holds the activation's turn for exactly
    /// as long as the scheduler would have held it for a real long-running
    /// method body - and holds nothing at all if the method interleaves, which
    /// is the difference the tests read.
    /// </summary>
    private sealed class RegistryEnumerationGatingFilter : IIncomingGrainCallFilter
    {
        public async Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            var gate = RegistryEnumerationGate.Gate;
            var method = context.InterfaceMethod;
            if (gate is not null &&
                method is not null &&
                method.Name == nameof(ILatticeRegistry.GetAllTreeIdsAsync) &&
                method.DeclaringType == typeof(ILatticeRegistry))
            {
                RegistryEnumerationGate.Entered?.TrySetResult();
                await gate.Task.ConfigureAwait(false);
            }

            await context.Invoke().ConfigureAwait(false);
        }
    }
}
