using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth.Tests.AdminAudit;

/// <summary>
/// The wiring shared by the admin audit trail tests: an in-memory audit tree, an
/// in-memory anchor tree, a fixed caller, and a controllable clock.
/// </summary>
internal sealed class AdminAuditHarness
{
    public const string AnchorTree = "sys-auth-policy";
    public const string AnchorKey = "tree\u001frule";

    public AdminAuditHarness(string subjectId = "alice", string region = "")
    {
        GrainFactory = Substitute.For<IGrainFactory>();
        GrainFactory.GetGrain<ILattice>(AuthConstants.AdminAuditTree).Returns(Audit.Lattice);
        GrainFactory.GetGrain<ILattice>(AnchorTree).Returns(Anchor.Lattice);
        GrainFactory.GetGrain<ISystemLattice>(Arg.Any<string>()).Returns(SystemAnchor);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ExistsAsync(Arg.Any<string>()).Returns(true);
        GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        Replication = Substitute.For<ILatticeReplicationContext>();
        Replication.LocalReplicaId.Returns(region);
        Membership = new FixedMembershipContext(subjectId);
        Options = new LatticeAuthOptions();
    }

    public IGrainFactory GrainFactory { get; }

    public InMemoryAuditLattice Audit { get; } = new();

    public InMemoryAuditLattice Anchor { get; } = new();

    public ISystemLattice SystemAnchor { get; } = Substitute.For<ISystemLattice>();

    public ILatticeReplicationContext Replication { get; }

    public FixedMembershipContext Membership { get; }

    public LatticeAuthOptions Options { get; }

    public FakeTimeProvider Time { get; } = new(new DateTimeOffset(2026, 11, 27, 12, 0, 0, TimeSpan.Zero));

    public static LatticeAdminAuditRequest Request(LatticeAdminAuditAnchor? anchor = null) =>
        new(
            LatticeAdminAuditAction.RuleUpsert,
            LatticeAdminAuditRequest.ClusterScope,
            LatticeAdminAuditTargetKind.Rule,
            "tree/rule",
            anchor ?? new LatticeAdminAuditAnchor(AnchorTree, AnchorKey),
            new Dictionary<string, string> { ["effect"] = "Allow" });

    public DurableLatticeAdminAuditTrail Trail() =>
        new(
            GrainFactory,
            new CovOptionsMonitor<LatticeAuthOptions>(Options),
            Replication,
            NullLogger<DurableLatticeAdminAuditTrail>.Instance,
            Membership,
            Time);

    public AdminAuditReconciler Reconciler() =>
        new(
            GrainFactory,
            new CovOptionsMonitor<LatticeAuthOptions>(Options),
            Replication,
            NullLogger<AdminAuditReconciler>.Instance,
            Time);

    /// <summary>A membership context that always resolves one subject.</summary>
    internal sealed class FixedMembershipContext(string subjectId) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(new LatticeSubject(subjectId));
    }

    /// <summary>A clock the test advances by hand.</summary>
    internal sealed class FakeTimeProvider(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset _now = now;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}
