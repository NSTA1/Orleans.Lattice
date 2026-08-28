using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the ordering contract between access-gate
/// authorization and per-tenant write admission on the <c>LatticeGrain</c>
/// data-plane choke point: <b>authorization must strictly precede admission</b>
/// at every user-origin write verb.
/// </summary>
/// <remarks>
/// <para>
/// The admission controller accounts against the ambient active tenant, which is
/// a client-supplied assertion carried in an unreserved request-context key that
/// the capability-stripping filter deliberately does not strip. Only the access
/// gate validates that assertion (by resolving the target tree's owning tenant
/// and refusing a subject that holds no membership of the tenant it named), so
/// consulting the controller first exposed a cross-tenant defect: an
/// unauthenticated or unauthorized caller could name any victim tenant and have a
/// stateful, quota-consuming, rate-limiting evaluation charged to that victim -
/// confirming the tenant's existence, draining its rate budget, and reading its
/// current usage and ceiling back out of the resulting quota exception.
/// </para>
/// <para>
/// These tests therefore assert two things per verb: the caller sees the
/// authorization denial (<see cref="LatticeAuthorizationDeniedException"/>, not
/// the tenancy refusal), and the admission controller is <b>never consulted at
/// all</b> - the observable proxy for "no victim tenant state was read or
/// mutated". They are deliberately behavioural rather than structural so they
/// keep holding if the verbs are refactored.
/// </para>
/// </remarks>
[TestFixture]
public class LatticeGrainTenantAdmissionOrderingTests
{
    private const string TreeId = "orders";

    /// <summary>A gate that denies every request, standing in for a caller with no grant on the target tree.</summary>
    private sealed class DenyingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Deny("denied by test"));
    }

    /// <summary>A gate that allows every request, used to prove admission still runs once authorization passes.</summary>
    private sealed class AllowingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    /// <summary>
    /// Records whether it was consulted. A non-zero <see cref="CallCount"/> after
    /// a denied call is precisely the defect these tests pin.
    /// </summary>
    private sealed class RecordingAdmissionController(bool admit) : ITenantAdmissionController
    {
        public bool IsActive => true;

        public int CallCount { get; private set; }

        public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
        {
            CallCount++;
            return new ValueTask<bool>(admit);
        }
    }

    private static (LatticeGrain grain, RecordingAdmissionController controller) CreateGrain(
        ILatticeAccessGate gate, bool admit = true)
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

        var controller = new RecordingAdmissionController(admit);
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITenantAdmissionController)).Returns(controller);
        services.GetService(typeof(ILatticeAccessGate)).Returns(gate);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, controller);
    }

    private static IEnumerable<TestCaseData> WriteVerbs()
    {
        yield return Verb("SetAsync", g => g.SetAsync("k", [1]));
        yield return Verb("SetAsync_with_ttl", g => g.SetAsync("k", [1], TimeSpan.FromMinutes(1)));
        yield return Verb("SetIfVersionAsync", g => g.SetIfVersionAsync("k", [1], HybridLogicalClock.Zero));
        yield return Verb("GetOrSetAsync", g => g.GetOrSetAsync("k", [1]));
        yield return Verb("SetManyAsync", g => g.SetManyAsync([new("k", [1])]));
        yield return Verb("SetManyAtomicAsync", g => g.SetManyAtomicAsync([new("k", [1])]));
        yield return Verb("SetManyAtomicAsync_with_operation_id", g => g.SetManyAtomicAsync([new("k", [1])], "op-1"));
        yield return Verb("SetManyAtomicAsync_with_deletes", g => g.SetManyAtomicAsync([new("k", [1])], ["d"], "op-2"));
        yield return Verb("DeleteAsync", g => g.DeleteAsync("k"));
        yield return Verb("DeleteRangeAsync", g => g.DeleteRangeAsync("a", "z"));
        yield return Verb("BulkLoadAsync", g => g.BulkLoadAsync([new("k", [1])]));
        yield return Verb("BulkAppendChunkAsync", g => g.BulkAppendChunkAsync("op-3", [new("k", [1])]));
    }

    private static TestCaseData Verb(string name, Func<ILattice, Task> op)
        => new TestCaseData(op).SetName(name);

    [TestCaseSource(nameof(WriteVerbs))]
    public void Denied_write_never_consults_the_admission_controller(Func<ILattice, Task> op)
    {
        var (grain, controller) = CreateGrain(new DenyingGate());

        // The caller asserts a tenant it holds no membership of. The gate is the
        // only component that validates that assertion, so it must run first.
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("victim"));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => op(grain));

        // The load-bearing assertion: no admission evaluation was charged to the
        // named victim tenant, so no quota counter was read, no rate-limit token
        // was consumed, and no usage or ceiling could leak back to the caller.
        Assert.That(controller.CallCount, Is.Zero,
            "admission must not be consulted for a request the access gate denies");
    }

    [TestCaseSource(nameof(WriteVerbs))]
    public void Authorized_write_still_consults_the_admission_controller(Func<ILattice, Task> op)
    {
        var (grain, controller) = CreateGrain(new AllowingGate());

        Assert.DoesNotThrowAsync(() => op(grain));

        // Reordering must not weaken the tenancy feature itself: once the gate
        // admits the call, the tenant quota seam is still consulted exactly as
        // before.
        Assert.That(controller.CallCount, Is.EqualTo(1));
    }

    [TestCaseSource(nameof(WriteVerbs))]
    public void Authorized_but_unadmitted_write_is_refused_by_the_tenancy_layer(Func<ILattice, Task> op)
    {
        var (grain, _) = CreateGrain(new AllowingGate(), admit: false);

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(() => op(grain));
    }
}
