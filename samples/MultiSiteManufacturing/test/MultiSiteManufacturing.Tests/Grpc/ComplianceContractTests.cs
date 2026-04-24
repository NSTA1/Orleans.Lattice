using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using MultiSiteManufacturing.Contracts.V1;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice.Primitives;
using DomainFacts = MultiSiteManufacturing.Host.Domain;
using ProtoV1 = MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Tests.Grpc;

/// <summary>
/// Contract-level tests for <c>ComplianceService</c> — exercises the
/// real gRPC pipeline via <see cref="GrpcContractFixture"/>'s in-process
/// channel. Covers both the point-in-time <c>GetPartCompliance</c>
/// response shape and the live <c>WatchDivergence</c> server stream.
/// </summary>
[TestFixture]
public sealed class ComplianceContractTests
{
    private GrpcContractFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new GrpcContractFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    [Test]
    public async Task GetPartCompliance_reports_nominal_for_agreed_part()
    {
        using var channel = _fixture.CreateChannel();
        var ingress = new FactIngressService.FactIngressServiceClient(channel);
        var compliance = new ComplianceService.ComplianceServiceClient(channel);

        const string serial = "HPT-BLD-S1-2028-80001";
        await ingress.EmitFactAsync(new FactEnvelope
        {
            FactId = Guid.NewGuid().ToString(),
            Serial = serial,
            Hlc = new ProtoV1.Hlc { WallClockTicks = 1, Counter = 0 },
            Site = ProtoV1.ProcessSite.OhioForge,
            OperatorId = "operator:test",
            Description = "forge",
            ProcessStepCompleted = new ProtoV1.ProcessStepCompleted { Stage = ProtoV1.ProcessStage.Forge },
        });

        var view = await compliance.GetPartComplianceAsync(new GetPartComplianceRequest { Serial = serial });

        Assert.That(view.Serial, Is.EqualTo(serial));
        Assert.That(view.BaselineState, Is.EqualTo(ProtoV1.ComplianceState.Nominal));
        Assert.That(view.LatticeState, Is.EqualTo(ProtoV1.ComplianceState.Nominal));
        Assert.That(view.Diverges, Is.False);
    }

    [Test]
    public async Task WatchDivergence_emits_report_when_backends_disagree()
    {
        using var channel = _fixture.CreateChannel();
        var compliance = new ComplianceService.ComplianceServiceClient(channel);

        // Reach into the host DI to grab the raw LatticeFactBackend so we
        // can sneak a fact past the router — this creates divergence
        // without relying on chaos presets (tier 2 flakiness is rate-based
        // and therefore non-deterministic in short-running contract tests).
        var lattice = _fixture.Services.GetRequiredService<LatticeFactBackend>();
        var router = _fixture.Services.GetRequiredService<FederationRouter>();

        const string serial = "HPT-BLD-S1-2028-80002";
        var partSerial = new PartSerialNumber(serial);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var call = compliance.WatchDivergence(new Empty(), cancellationToken: cts.Token);
        var reader = call.ResponseStream;

        // Kick off the first MoveNext so the subscription is registered
        // server-side before we emit the divergent facts.
        var firstMove = reader.MoveNext(cts.Token);
        await Task.Delay(200, cts.Token);

        // Step 1 — lattice sees a Critical NC, baseline doesn't.
        await lattice.EmitAsync(new DomainFacts.NonConformanceRaised
        {
            Serial = partSerial,
            FactId = Guid.NewGuid(),
            Hlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            Site = DomainFacts.ProcessSite.ToulouseNdtLab,
            Operator = DomainFacts.OperatorId.Demo,
            Description = "critical NC",
            NcNumber = "NC-80002",
            DefectCode = "D-001",
            Severity = DomainFacts.NcSeverity.Critical,
        }, cts.Token);

        // Step 2 — route any fact through the router so FactRouted fires
        // and the broadcaster re-reads both backends, detecting divergence.
        await router.EmitAsync(new DomainFacts.ProcessStepCompleted
        {
            Serial = partSerial,
            FactId = Guid.NewGuid(),
            Hlc = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 },
            Site = DomainFacts.ProcessSite.OhioForge,
            Operator = DomainFacts.OperatorId.Demo,
            Description = "forge",
            Stage = DomainFacts.ProcessStage.Forge,
        });

        var hasMessage = await firstMove;
        Assert.That(hasMessage, Is.True, "Expected at least one DivergenceReport on the stream.");

        var report = reader.Current;
        Assert.That(report.Serial, Is.EqualTo(serial));
        Assert.That(report.LatticeState, Is.EqualTo(ProtoV1.ComplianceState.Scrap));
        Assert.That(report.BaselineState, Is.EqualTo(ProtoV1.ComplianceState.Nominal));
        Assert.That(report.Resolved, Is.False);
    }
}
