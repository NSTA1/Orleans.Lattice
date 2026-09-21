using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Proves issue #3238 against a real Orleans activation stack: a whole-registry
/// enumeration that has its underlying async-enumerable aborted mid-scan must
/// still return the complete, exact catalog.
/// <para>
/// The production trigger is <c>[StatelessWorker]</c> worker mis-routing.
/// <c>LatticeGrain</c> - which backs the registry's own <c>_lattice_trees</c>
/// tree - is <c>[StatelessWorker(maxLocalWorkers: 32)]</c>. Orleans holds
/// async-enumerable state in a dictionary on the activation that served
/// <c>StartEnumeration</c>, but routes every subsequent <c>MoveNext</c> as an
/// independent message with no request affinity, so a <c>MoveNext</c> that
/// lands on a sibling worker finds no enumerator and the call aborts.
/// </para>
/// <para>
/// This fixture injects the abort at the real Orleans grain-call seam rather
/// than racing worker selection. That is deliberate. Reproducing genuine
/// mis-routing requires a second worker activation to exist <i>and</i> to win
/// the routing decision for an in-flight enumeration, neither of which a test
/// can force - so a concurrency-based reproduction would be a probabilistic
/// detector that passes against the unfixed code whenever the race does not
/// land, which is the precise shape of a false green. Injecting
/// <c>EnumerationAbortedException</c> from an
/// <see cref="IIncomingGrainCallFilter"/> on the enumeration's own
/// <c>MoveNext</c> produces the identical observable, deterministically, while
/// still exercising the real <c>LatticeRegistryGrain</c>,
/// <c>LatticeGrain</c> and Orleans streaming stack end to end - only the
/// <i>cause</i> is simulated, never the recovery path under test.
/// </para>
/// <para>
/// The assertions are on the scan's <i>contents</i>, not merely on it not
/// throwing: the resume path's entire job is exactness across a reopen, and a
/// dropped or repeated tree id is the failure that would actually corrupt a WAL
/// GC pass. Each test also asserts the abort was really injected, so a filter
/// that silently stopped matching cannot leave the fixture passing vacuously.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeRegistryEnumerationAbortRecoveryTests
{
    private static readonly string[] Trees =
    [
        "registry-abort-tree-a",
        "registry-abort-tree-b",
        "registry-abort-tree-c",
        "registry-abort-tree-d",
    ];

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var registry = Registry();
        foreach (var tree in Trees)
        {
            await registry.RegisterAsync(tree);
        }

        // Warm the enumeration path once, ungated, so the tests below are not
        // measuring first-touch activation of the backing tree's grains.
        await registry.GetAllTreeIdsAsync(null).WaitAsync(TimeSpan.FromSeconds(60));
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        EnumerationAbortGate.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void Disarm() => EnumerationAbortGate.Reset();

    /// <summary>
    /// The acceptance criterion for #3238. Against the unfixed code the abort
    /// propagates out of <c>GetAllTreeIdsAsync</c>, the WAL GC scheduler
    /// catches it above its per-tree loop, and the entire pass is abandoned -
    /// every tree loses its collection, not one.
    /// </summary>
    [Test]
    public async Task GetAllTreeIdsAsync_returns_the_complete_catalog_when_the_enumeration_aborts_mid_scan()
    {
        EnumerationAbortGate.ArmAfter(0);

        var result = await Registry().GetAllTreeIdsAsync(null).WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(EnumerationAbortGate.Fired, Is.True,
            "The abort was never injected, so this fixture proved nothing. The Orleans async-enumerable " +
            "MoveNext seam the filter matches on has probably changed. MoveNext calls seen: " +
            EnumerationAbortGate.MoveNextCount);
        Assert.That(result, Is.SupersetOf(Trees),
            "Every registered tree must survive an abort mid-enumeration. A missing id here is a tree " +
            "whose WAL would never be collected.");
        Assert.That(result, Is.Unique,
            "The resume must reopen strictly past the last yielded key - a duplicate id means the " +
            "reopen re-read a key it had already returned.");
        Assert.That(result, Is.Ordered,
            "The reopen must preserve the scan's ordering rather than restarting it.");
    }

    /// <summary>
    /// Recovery must survive more than one abort, since the mis-route
    /// probability applies afresh to every <c>MoveNext</c> the reopened
    /// enumeration issues - a reopen is not immune, merely independently
    /// likely to succeed.
    /// </summary>
    [Test]
    public async Task GetAllTreeIdsAsync_recovers_from_repeated_aborts_within_the_reconnect_budget()
    {
        EnumerationAbortGate.ArmAfter(0, repeats: 3);

        var result = await Registry().GetAllTreeIdsAsync(null).WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(EnumerationAbortGate.FireCount, Is.EqualTo(3),
            "All three aborts should have been injected.");
        Assert.That(result, Is.SupersetOf(Trees));
        Assert.That(result, Is.Unique);
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
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, EnumerationAbortInjectingFilter>();
        }
    }

    /// <summary>
    /// Control state for the injected abort. Static because the TestingHost
    /// silo runs in-process; disarmed after every test so it cannot leak.
    /// </summary>
    private static class EnumerationAbortGate
    {
        private static int _allowBeforeAbort;
        private static int _remainingAborts;
        private static int _observedMoveNext;
        private static int _fireCount;
        private static int _moveNextCount;

        internal static bool Fired => Volatile.Read(ref _fireCount) > 0;

        internal static int MoveNextCount => Volatile.Read(ref _moveNextCount);

        internal static void CountMoveNext() => Interlocked.Increment(ref _moveNextCount);

        internal static int FireCount => Volatile.Read(ref _fireCount);

        internal static void ArmAfter(int successfulMoveNexts, int repeats = 1)
        {
            Volatile.Write(ref _observedMoveNext, 0);
            Volatile.Write(ref _fireCount, 0);
            Volatile.Write(ref _moveNextCount, 0);
            Volatile.Write(ref _allowBeforeAbort, successfulMoveNexts);
            Volatile.Write(ref _remainingAborts, repeats);
        }

        internal static void Reset()
        {
            Volatile.Write(ref _remainingAborts, 0);
            Volatile.Write(ref _allowBeforeAbort, 0);
            Volatile.Write(ref _observedMoveNext, 0);
        }

        /// <summary>
        /// Returns true when this <c>MoveNext</c> should abort. Each abort
        /// resets the allowance so a reopened enumeration makes real progress
        /// before the next injection, which is what lets the repeated-abort
        /// test terminate instead of livelocking.
        /// </summary>
        internal static bool ShouldAbort()
        {
            if (Volatile.Read(ref _remainingAborts) <= 0)
            {
                return false;
            }
            if (Interlocked.Increment(ref _observedMoveNext) <= Volatile.Read(ref _allowBeforeAbort))
            {
                return false;
            }

            Interlocked.Decrement(ref _remainingAborts);
            Interlocked.Increment(ref _fireCount);
            Volatile.Write(ref _observedMoveNext, 0);
            return true;
        }
    }

    /// <summary>
    /// Throws <see cref="EnumerationAbortedException"/> from the registry
    /// backing tree's async-enumerable <c>MoveNext</c>, which is the exact
    /// observable a <c>MoveNext</c> mis-routed to a sibling stateless worker
    /// produces (Orleans raises it on <c>MissingEnumeratorError</c>).
    /// <para>
    /// Scoped to the <c>_lattice_trees</c> grain key so no unrelated
    /// enumeration in the silo is disturbed.
    /// </para>
    /// </summary>
    private sealed class EnumerationAbortInjectingFilter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            var name = context.InterfaceMethod?.Name;
            if (name == "MoveNext" &&
                context.TargetContext?.GrainId.Key.ToString() == LatticeConstants.RegistryTreeId)
            {
                EnumerationAbortGate.CountMoveNext();
                if (EnumerationAbortGate.ShouldAbort())
                {
                    throw new EnumerationAbortedException();
                }
            }

            return context.Invoke();
        }
    }
}
