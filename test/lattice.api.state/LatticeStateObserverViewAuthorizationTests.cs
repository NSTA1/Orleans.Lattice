using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Regression coverage for the tree a change-feed subscription is authorized
/// against.
///
/// The change feed tails the write-ahead log directly rather than flowing
/// through the gated <see cref="ILattice"/> surface, so
/// <see cref="LatticeStateObserver"/> is the sole point that honours the
/// data-plane read policy for the live stream: nothing downstream can
/// compensate. A materialised-view (<c>view-*</c>) tree id is a caller-supplied
/// name for someone else's data, and <c>view-</c> is deliberately not a reserved
/// prefix, so a grant on the view - or a whole-estate <c>Tree:*</c> grant, which
/// reaches a view precisely because it is unreserved - says nothing about who may
/// read the source. Authorizing a view subscription against the view id therefore
/// asked a question whose answer could not protect the source, and streamed every
/// key, change kind and HLC timestamp of the source data to a subject with no
/// grant on it (and past an explicit deny on it, which was never evaluated).
///
/// The rule these tests pin is the one the read facade already states and
/// implements (<c>LatticeStateQuery.IsTreeReadHiddenAsync</c> /
/// <c>ResolveViewKeyFilterAsync</c>): a view is decided by the readability of its
/// SOURCE, and an unresolvable source fails closed.
/// </summary>
[TestFixture]
public sealed class LatticeStateObserverViewAuthorizationTests
{
    private const string ViewName = "orders-by-customer";
    private const string ViewTree = LatticeConstants.ViewTreePrefix + ViewName;
    private const string SourceTree = "orders";

    /// <summary>
    /// Records every tree id the gate is asked to authorize, so a test can assert
    /// <em>which</em> tree the decision was made about rather than only whether it
    /// was allowed. That distinction is the whole vulnerability: deciding the
    /// right way about the wrong tree still leaks the source.
    /// </summary>
    private sealed class RecordingGate(string[] readableTrees) : ILatticeAccessGate
    {
        public List<string> AskedAbout { get; } = [];

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            AskedAbout.Add(request.TreeId);
            return new(readableTrees.Contains(request.TreeId, StringComparer.Ordinal)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not readable"));
        }
    }

    private sealed class FixedSubject : ILatticeMembershipContext
    {
        private static readonly LatticeSubject Subject = new("alice");

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(Subject);
    }

    private sealed class StubViewCatalog((string ViewName, string SourceTreeId)[] views) : IViewCatalog
    {
        public ViewRegistration? TryGet(string viewName)
        {
            foreach (var (name, source) in views)
            {
                if (string.Equals(name, viewName, StringComparison.Ordinal))
                {
                    return new ViewRegistration(name, source, Projection: null);
                }
            }

            return null;
        }

        public void Register(ViewRegistration registration) => throw new NotSupportedException();

        public void Remove(string viewName) => throw new NotSupportedException();

        public IReadOnlyCollection<ViewRegistration> All() => throw new NotSupportedException();
    }

    /// <summary>
    /// Builds an observer whose subject may read exactly
    /// <paramref name="readableTrees"/>, over a tree that exists and an idle
    /// write-ahead log. <paramref name="localViews"/> seeds the silo-local view
    /// catalog; <paramref name="runtimeViews"/> seeds the durable cluster
    /// registry; <paramref name="registryThrows"/> makes the registry activation
    /// fail.
    /// </summary>
    private static (LatticeStateObserver Observer, RecordingGate Gate) CreateObserver(
        string[] readableTrees,
        (string ViewName, string SourceTreeId)[]? localViews = null,
        (string ViewName, string SourceTreeId)[]? runtimeViews = null,
        bool registryThrows = false)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree);

        var treeRegistry = Substitute.For<ILatticeRegistry>();
        treeRegistry.ResolveAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<string>(null!));
        treeRegistry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(treeRegistry);

        // An idle WAL: the subscription seeds at sequence 0 and every poll
        // returns an empty page, so an admitted subscription stays open and
        // delivers nothing until the caller cancels.
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetNextSequenceAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<long>(0L));
        wal.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(new WalShardPage
            {
                Entries = [],
                NextSequence = 0,
            }));
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(wal);

        var viewRegistry = Substitute.For<IViewRegistryGrain>();
        if (registryThrows)
        {
            viewRegistry.ListAsync().Returns<Task<IReadOnlyList<RuntimeViewRegistration>>>(
                _ => throw new InvalidOperationException("registry activation failed"));
        }
        else
        {
            IReadOnlyList<RuntimeViewRegistration> registrations = (runtimeViews ?? [])
                .Select(v => new RuntimeViewRegistration
                {
                    ViewName = v.ViewName,
                    SourceTreeId = v.SourceTreeId,
                    ProjectionTypeName = "StubProjection",
                    ProjectionVersion = "1",
                })
                .ToList();
            viewRegistry.ListAsync().Returns(Task.FromResult(registrations));
        }

        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(viewRegistry);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = 1 });

        var gate = new RecordingGate(readableTrees);
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAccessGate>(gate);
        services.AddSingleton<ILatticeMembershipContext>(new FixedSubject());
        if (localViews is not null)
        {
            services.AddSingleton<IViewCatalog>(new StubViewCatalog(localViews));
        }

        var observer = new LatticeStateObserver(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions
            {
                ChangeObservationPollInterval = TimeSpan.FromMilliseconds(5),
            }),
            services.BuildServiceProvider());

        return (observer, gate);
    }

    /// <summary>
    /// Opens the subscription and pumps it briefly. Returns the terminal
    /// exception, or <see langword="null"/> when the subscription was admitted and
    /// simply idled until cancellation.
    /// </summary>
    private static async Task<Exception?> SubscribeAsync(LatticeStateObserver observer, string treeId)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));
        try
        {
            await foreach (var _ in observer.ObserveAsync(new StateObserveRequest { TreeId = treeId }, cts.Token))
            {
                break;
            }

            return null;
        }
        catch (OperationCanceledException)
        {
            // Admitted: the feed stayed open over an idle WAL until the pump
            // deadline elapsed.
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    [Test]
    public async Task A_view_grant_alone_does_not_admit_a_subscription_over_an_unreadable_source()
    {
        // The attack: the subject holds read on the view (as a Tree:* grant would
        // also confer, view- being unreserved) but none on the source it mirrors.
        var (observer, gate) = CreateObserver(
            readableTrees: [ViewTree],
            localViews: [(ViewName, SourceTree)]);

        var error = await SubscribeAsync(observer, ViewTree);

        Assert.Multiple(() =>
        {
            Assert.That(error, Is.TypeOf<KeyNotFoundException>(),
                "A view subscription whose source the subject cannot read must be refused, " +
                "not streamed straight off the write-ahead log.");
            Assert.That(gate.AskedAbout, Does.Contain(SourceTree),
                "The subscription must be decided against the view's source tree.");
            Assert.That(gate.AskedAbout, Does.Not.Contain(ViewTree),
                "Deciding against the view id asks a question that cannot protect the source.");
        });
    }

    [Test]
    public async Task A_view_subscription_is_admitted_on_a_source_grant()
    {
        // The converse control: the same view, the same absent grant on the view
        // id itself, but a real grant on the source - which is the grant that
        // actually governs the data being streamed.
        var (observer, gate) = CreateObserver(
            readableTrees: [SourceTree],
            localViews: [(ViewName, SourceTree)]);

        var error = await SubscribeAsync(observer, ViewTree);

        Assert.Multiple(() =>
        {
            Assert.That(error, Is.Null, "A subject that may read the source may observe the view.");
            Assert.That(gate.AskedAbout, Does.Contain(SourceTree));
        });
    }

    [Test]
    public async Task An_unknown_view_is_refused_rather_than_streamed_ungated()
    {
        // Neither the local catalog nor the cluster registry knows the view, so
        // the source - the subscription's only authorization boundary - cannot be
        // resolved. Proceeding would turn an unresolvable view into an ungated
        // tail of whatever tree backs it.
        var (observer, _) = CreateObserver(readableTrees: [SourceTree, ViewTree]);

        var error = await SubscribeAsync(observer, ViewTree);

        Assert.That(error, Is.TypeOf<KeyNotFoundException>());
    }

    [Test]
    public async Task A_transient_view_registry_failure_refuses_the_subscription()
    {
        var (observer, _) = CreateObserver(readableTrees: [SourceTree, ViewTree], registryThrows: true);

        var error = await SubscribeAsync(observer, ViewTree);

        Assert.That(error, Is.TypeOf<KeyNotFoundException>(),
            "A registry blip must fail closed; degrading to an ungated tail is a privilege escalation.");
    }

    [Test]
    public async Task A_runtime_view_created_on_another_silo_is_still_decided_by_its_source()
    {
        // No local catalog entry: the silo-local fast path misses and the durable
        // cluster registry must supply the source, which is then gated like any
        // other. Resolving a source is not the same as granting it.
        var (observer, gate) = CreateObserver(
            readableTrees: [ViewTree],
            runtimeViews: [(ViewName, SourceTree)]);

        var error = await SubscribeAsync(observer, ViewTree);

        Assert.Multiple(() =>
        {
            Assert.That(error, Is.TypeOf<KeyNotFoundException>());
            Assert.That(gate.AskedAbout, Does.Contain(SourceTree));
        });
    }

    [Test]
    public async Task A_view_tree_id_carrying_no_recoverable_name_is_refused()
    {
        var (observer, _) = CreateObserver(readableTrees: [SourceTree, LatticeConstants.ViewTreePrefix]);

        var error = await SubscribeAsync(observer, LatticeConstants.ViewTreePrefix);

        Assert.That(error, Is.TypeOf<KeyNotFoundException>());
    }

    [Test]
    public async Task An_ordinary_tree_is_still_decided_against_itself()
    {
        // The source-resolution step must not disturb the ordinary path: a
        // non-view tree is authorized by its own grant, exactly as before.
        var (admitted, admittedGate) = CreateObserver(readableTrees: [SourceTree]);
        var (refused, _) = CreateObserver(readableTrees: ["something-else"]);

        var admittedError = await SubscribeAsync(admitted, SourceTree);
        var refusedError = await SubscribeAsync(refused, SourceTree);

        Assert.Multiple(() =>
        {
            Assert.That(admittedError, Is.Null);
            Assert.That(admittedGate.AskedAbout, Is.EqualTo(new[] { SourceTree }));
            Assert.That(refusedError, Is.TypeOf<KeyNotFoundException>());
        });
    }
}
