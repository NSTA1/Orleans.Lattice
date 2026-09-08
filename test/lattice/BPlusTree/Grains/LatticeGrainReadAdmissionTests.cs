using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for per-tenant admission on the <b>read</b> plane.
/// </summary>
/// <remarks>
/// <para>
/// Per-tenant admission was reachable only from the write-mutation sites
/// (<c>ThrowIfWriteNotAdmittedAsync</c>), so the sustained request-rate
/// dimension - documented as a cluster-wide operations-per-second ceiling -
/// silently governed writes alone. A tenant could therefore issue unbounded
/// reads, including whole-keyspace counts, diagnostics and range scans, with no
/// budget and no fairness of any kind against its neighbours: the read plane had
/// no throttle, so a single tenant hammering a shared tree could starve every
/// other tenant of responsive service while remaining comfortably inside every
/// quota it was nominally subject to.
/// </para>
/// <para>
/// These tests pin three properties per read verb. First, an authorized read
/// consults the admission controller at all (before the fix the call count was
/// zero for every verb - the defect). Second, a refusal is surfaced to the
/// caller as <see cref="LatticeTenantAccessDeniedException"/> rather than being
/// silently ignored. Third - and this is the security half - a read the access
/// gate <em>denies</em> must never reach the controller, because the tenant a
/// read is charged to is a caller-supplied assertion that only the gate
/// validates; charging first would let an unauthorized caller drain any victim
/// tenant's read budget, turning the quota system into the very denial-of-service
/// vector it exists to prevent.
/// </para>
/// <para>
/// They are deliberately behavioural rather than structural, and they cover both
/// enforcement shapes the read surface uses (the local <c>AuthorizeAsync</c>
/// choke point and the static <c>LatticeAccessGateEnforcement</c> helpers), so
/// they keep holding if the verbs are refactored between the two.
/// </para>
/// </remarks>
[TestFixture]
public class LatticeGrainReadAdmissionTests
{
    private const string TreeId = "orders";

    /// <summary>A gate that allows every request, standing in for an authorized caller.</summary>
    private sealed class AllowingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    /// <summary>A gate that denies every request, standing in for a caller with no grant on the tree.</summary>
    private sealed class DenyingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Deny("denied by test"));
    }

    /// <summary>
    /// Records how many times the <em>read</em> admission seam was consulted, and
    /// with which arguments, so a test can assert both that it ran and that the
    /// active tenant and tree id were threaded through.
    /// </summary>
    private sealed class RecordingAdmissionController(bool active, bool admitRead) : ITenantAdmissionController
    {
        public bool IsActive => active;

        public int ReadCallCount { get; private set; }

        public int WriteCallCount { get; private set; }

        public TenantId LastTenant { get; private set; }

        public string? LastTreeId { get; private set; }

        public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
        {
            WriteCallCount++;
            return new ValueTask<bool>(true);
        }

        public bool IsReadAdmitted(TenantId tenant, string treeId)
        {
            ReadCallCount++;
            LastTenant = tenant;
            LastTreeId = treeId;
            return admitRead;
        }
    }

    private static (LatticeGrain grain, RecordingAdmissionController controller) CreateGrain(
        ILatticeAccessGate gate, bool active = true, bool admitRead = true)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var controller = new RecordingAdmissionController(active, admitRead);
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITenantAdmissionController)).Returns(controller);
        services.GetService(typeof(ILatticeAccessGate)).Returns(gate);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, controller);
    }

    /// <summary>
    /// Read verbs spanning both enforcement shapes: <c>GetAsync</c> and
    /// <c>TreeExistsAsync</c> flow through the local <c>AuthorizeAsync</c> choke
    /// point, while the counts and the operational reports enforce through the
    /// static <c>LatticeAccessGateEnforcement</c> helpers. Both had to be charged
    /// separately, so both are pinned here.
    /// </summary>
    private static IEnumerable<TestCaseData> ReadVerbs()
    {
        yield return Verb("GetAsync", g => g.GetAsync("k"));
        yield return Verb("TreeExistsAsync", g => g.TreeExistsAsync());
        yield return Verb("CountAsync", g => g.CountAsync());
        yield return Verb("CountAsync_over_range", g => g.CountAsync("a", "z"));
        yield return Verb("GetStorageUsageAsync", g => g.GetStorageUsageAsync());
        yield return Verb("GetHistoryRetentionAsync", g => g.GetHistoryRetentionAsync());
        yield return Verb("WarmUpAsync", g => g.WarmUpAsync());
    }

    private static TestCaseData Verb(string name, Func<ILattice, Task> op)
        => new TestCaseData(op).SetName(name);

    // ----- The defect: reads were never charged at all -----

    [TestCaseSource(nameof(ReadVerbs))]
    public void Authorized_read_consults_the_admission_controller(Func<ILattice, Task> op)
    {
        var (grain, controller) = CreateGrain(new AllowingGate());

        Assert.DoesNotThrowAsync(() => op(grain));

        // Before the read plane was wired to admission this was zero for every
        // verb: the request-rate budget simply did not apply to reads.
        Assert.That(controller.ReadCallCount, Is.GreaterThanOrEqualTo(1),
            "an authorized read must be charged against the tenant's admission budget");
        Assert.That(controller.LastTreeId, Is.EqualTo(TreeId));
    }

    [TestCaseSource(nameof(ReadVerbs))]
    public void Unadmitted_read_is_refused_by_the_tenancy_layer(Func<ILattice, Task> op)
    {
        var (grain, _) = CreateGrain(new AllowingGate(), admitRead: false);

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(() => op(grain));
    }

    // ----- The security half: ordering must match the write plane -----

    [TestCaseSource(nameof(ReadVerbs))]
    public void Denied_read_never_consults_the_admission_controller(Func<ILattice, Task> op)
    {
        var (grain, controller) = CreateGrain(new DenyingGate());

        // The caller asserts a tenant it holds no membership of. Only the gate
        // validates that assertion, so the gate must run first.
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("victim"));

        // A denied read fails in whichever way its own disclosure semantics
        // dictate - a hard deny throws, a filterable one prunes every key and
        // returns empty - and under these substituted shards a pruned enumeration
        // can fault on the stub itself. None of that is what this test is about,
        // so any outcome is accepted; the single load-bearing assertion is that
        // the named victim tenant was not charged.
        try
        {
            op(grain).GetAwaiter().GetResult();
        }
        catch (Exception)
        {
            // Intentionally ignored - see above.
        }

        Assert.That(controller.ReadCallCount, Is.Zero,
            "a read the access gate denies must not consume the named tenant's budget");
    }

    // ----- Inert unless tenancy is actually registered and active -----

    [TestCaseSource(nameof(ReadVerbs))]
    public void Read_with_inactive_controller_proceeds_without_consulting_it(Func<ILattice, Task> op)
    {
        var (grain, controller) = CreateGrain(new AllowingGate(), active: false, admitRead: false);

        Assert.DoesNotThrowAsync(() => op(grain));
        Assert.That(controller.ReadCallCount, Is.Zero);
    }

    [Test]
    public void Read_with_no_controller_registered_proceeds()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ILatticeAccessGate)).Returns(new AllowingGate());

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);

        Assert.DoesNotThrowAsync(() => grain.GetAsync("k"));
        Assert.DoesNotThrowAsync(() => grain.CountAsync());
    }

    // ----- System-origin traffic is exempt, as on the write plane -----

    [Test]
    public void System_origin_read_is_not_charged()
    {
        var (grain, controller) = CreateGrain(new AllowingGate(), admitRead: false);

        using var _system = LatticeAccessGateContext.EnterSystemOrigin();

        // Internal machinery must not be refused by, or accounted against, a
        // tenant's request-rate budget.
        Assert.DoesNotThrowAsync(() => grain.CountAsync());
        Assert.That(controller.ReadCallCount, Is.Zero);
    }
}
