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
/// The two tests isolate the two halves of the fix, and neither subsumes the
/// other. The reader test is the literal acceptance criterion and holds as long
/// as <i>either</i> the reader or the enumeration interleaves, so it goes red
/// only when both attributes are removed. The mutator test pins the
/// enumeration's own attribute on its own, because a non-interleaving mutator
/// queues behind any held turn no matter what the reader is marked.
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
    /// Pins the enumeration's own interleave attribute. A mutator carries no
    /// interleave attribute by design, so it is admitted only when the
    /// activation holds no turn - which makes it a direct probe of whether the
    /// parked enumeration is holding one.
    /// </summary>
    [Test]
    public async Task Registration_completes_while_a_whole_registry_enumeration_is_parked()
    {
        var registry = Registry();
        RegistryEnumerationGate.Arm();

        Task<IReadOnlyList<string>>? blocker = null;
        try
        {
            blocker = registry.GetAllTreeIdsAsync(null);
            await RegistryEnumerationGate.Entered!.Task.WaitAsync(TimeSpan.FromSeconds(30));

            var register = registry.RegisterAsync(LateRegisteredTree);
            var winner = await Task.WhenAny(register, Task.Delay(TimeSpan.FromSeconds(20)));

            Assert.That(winner, Is.SameAs(register),
                "RegisterAsync carries no interleave attribute, so it is admitted only when the registry " +
                "activation holds no turn. It queued here, which means GetAllTreeIdsAsync is still " +
                "holding the singleton's turn for the length of its fan-out - the exact head-of-line " +
                "block of issue #3180.");
            await register;
        }
        finally
        {
            RegistryEnumerationGate.Release();
            if (blocker is not null)
            {
                await blocker.WaitAsync(TimeSpan.FromSeconds(60));
            }
            await registry.UnregisterAsync(LateRegisteredTree);
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
