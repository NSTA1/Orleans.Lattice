using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Wal;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for the tenant request-rate charge on
/// <see cref="LatticeBackupCaptureService"/>.
/// </summary>
/// <remarks>
/// <para>
/// A capture is the largest tenant-triggerable read the platform offers: it drains
/// the whole of a pinned cut through the raw-entry seam. Every page of that drain
/// runs inside a system-origin scope - necessary, because the collector reads
/// snapshot leaf grains as infrastructure - which has the side effect that the
/// data-plane read charge never observes it. A tenant could therefore drive an
/// unbounded sequence of full-keyspace scans at no budgetary cost: the same
/// noisy-neighbour hole the read charge closes, reached through a different verb.
/// </para>
/// <para>
/// The charge is therefore taken at the capture seam instead, and these tests pin
/// the three properties that make it correct: it is taken (so the vector is
/// closed), it is taken strictly after authorization (so an unauthorized caller
/// cannot drive a stateful, rate-consuming evaluation against a victim tenant it
/// merely names), and it is inert when tenancy is not registered.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeBackupCaptureAdmissionTests
{
    private const string Tree = "orders";

    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    /// <summary>
    /// An admission controller that records every read charge and returns a fixed
    /// decision, so both "was it charged" and "was it charged before the gate ran"
    /// are directly observable.
    /// </summary>
    private sealed class RecordingAdmissionController(bool active, bool admitRead) : ITenantAdmissionController
    {
        public int ReadCallCount { get; private set; }

        public bool IsActive => active;

        public bool IsReadAdmitted(TenantId tenant, string treeId)
        {
            ReadCallCount++;
            return admitRead;
        }

        public ValueTask<bool> IsAdmittedAsync(
            TenantId tenant, string treeId, CancellationToken cancellationToken = default) =>
            new(true);
    }

    private LatticeBackupCaptureService CreateService(
        RecordingAdmissionController controller,
        bool gateAllows = true)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var gate = Substitute.For<ILatticeAccessGate>();
        gate.AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<LatticeAccessDecision>(
                gateAllows
                    ? LatticeAccessDecision.Allow()
                    : LatticeAccessDecision.Deny("denied by test")));

        return new LatticeBackupCaptureService(
            grainFactory,
            Substitute.For<ILatticeBackupSink>(),
            Substitute.For<ILatticeBackupCatalogStore>(),
            new BackupAccessAuthorizer(gate),
            optionsMonitor,
            Options.Create(new LatticeBackupOptions()),
            Substitute.For<ILatticeMergeModeResolver>(),
            _serializer,
            Substitute.For<ICommitLogReader>(),
            Substitute.For<IWalSubscriber>(),
            new LatticeOptionsResolver(grainFactory, optionsMonitor),
            Substitute.For<IWalCursorRegistry>(),
            Options.Create(new ClusterOptions()),
            NullLogger<LatticeBackupCaptureService>.Instance,
            controller);
    }

    /// <summary>
    /// The defect: an authorized capture was never charged, so repeated captures
    /// were an unbilled full-keyspace scan a tenant could drive at will.
    /// </summary>
    [Test]
    public void An_authorized_capture_is_charged_against_the_tenant_budget()
    {
        var controller = new RecordingAdmissionController(active: true, admitRead: true);
        var service = CreateService(controller);

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        // The capture proceeds past the charge and fails later against the
        // substituted cluster; the charge itself is what is under test.
        Assert.That(async () => await service.CaptureAsync(NewRequest()), Throws.Exception);
        Assert.That(controller.ReadCallCount, Is.EqualTo(1));
    }

    [Test]
    public void A_capture_over_the_rate_budget_is_refused()
    {
        var controller = new RecordingAdmissionController(active: true, admitRead: false);
        var service = CreateService(controller);

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        Assert.That(
            async () => await service.CaptureAsync(NewRequest()),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    /// <summary>
    /// Authorize-then-account: the active tenant is a caller assertion that only
    /// the authorizer validates, so a denied caller must never reach a stateful,
    /// rate-consuming evaluation charged to the tenant it merely named.
    /// </summary>
    [Test]
    public void A_capture_the_authorizer_denies_is_never_charged()
    {
        var controller = new RecordingAdmissionController(active: true, admitRead: true);
        var service = CreateService(controller, gateAllows: false);

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("victim"));

        Assert.That(
            async () => await service.CaptureAsync(NewRequest()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(
            controller.ReadCallCount,
            Is.Zero,
            "a denied caller must not be able to charge a tenant it merely asserted");
    }

    /// <summary>
    /// Infrastructure-authored captures - a scheduled backup - run system-origin
    /// and are exempt, exactly as they are from every other tenant charge.
    /// </summary>
    [Test]
    public void A_system_origin_capture_is_not_charged()
    {
        var controller = new RecordingAdmissionController(active: true, admitRead: false);
        var service = CreateService(controller);

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();

        Assert.That(CaptureFailure(service), Is.Not.InstanceOf<LatticeTenantAccessDeniedException>());
        Assert.That(controller.ReadCallCount, Is.Zero);
    }

    /// <summary>
    /// Tenancy is an optional add-on: with no controller registered, or an inactive
    /// one, the capture path must be untouched.
    /// </summary>
    [Test]
    public void An_inactive_controller_does_not_charge()
    {
        var controller = new RecordingAdmissionController(active: false, admitRead: false);
        var service = CreateService(controller);

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        Assert.That(CaptureFailure(service), Is.Not.InstanceOf<LatticeTenantAccessDeniedException>());
        Assert.That(controller.ReadCallCount, Is.Zero);
    }

    private static LatticeBackupCaptureRequest NewRequest() =>
        new("backup-1", BackupScopeSelector.WholeTree(Tree));

    /// <summary>
    /// Runs a capture that is expected to fail somewhere past the charge (the
    /// cluster is substituted) and returns the exception, so a test can assert on
    /// its type without asserting that the capture succeeds.
    /// </summary>
    private static Exception? CaptureFailure(LatticeBackupCaptureService service)
    {
        try
        {
            service.CaptureAsync(NewRequest()).GetAwaiter().GetResult();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
