using MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Tests.Grpc;

[TestFixture]
public sealed class FactIngressContractTests
{
    private GrpcContractFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new GrpcContractFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    [Test]
    public async Task EmitFact_accepts_process_step_completed()
    {
        using var channel = _fixture.CreateChannel();
        var client = new FactIngressService.FactIngressServiceClient(channel);

        var envelope = new FactEnvelope
        {
            FactId = Guid.NewGuid().ToString(),
            Serial = "HPT-BLD-S1-2028-00001",
            Hlc = new Hlc { WallClockTicks = 10, Counter = 0 },
            Site = ProcessSite.OhioForge,
            OperatorId = "operator:test",
            Description = "forge complete",
            ProcessStepCompleted = new ProcessStepCompleted
            {
                Stage = ProcessStage.Forge,
                HeatLot = "HL-001",
            },
        };

        var result = await client.EmitFactAsync(envelope);

        Assert.That(result.Accepted, Is.True);
        Assert.That(result.FactId, Is.EqualTo(envelope.FactId));
    }

    [Test]
    public async Task EmitFact_then_GetPartCompliance_reports_nominal()
    {
        using var channel = _fixture.CreateChannel();
        var ingress = new FactIngressService.FactIngressServiceClient(channel);
        var compliance = new ComplianceService.ComplianceServiceClient(channel);

        const string serial = "HPT-BLD-S1-2028-00002";
        await ingress.EmitFactAsync(new FactEnvelope
        {
            FactId = Guid.NewGuid().ToString(),
            Serial = serial,
            Hlc = new Hlc { WallClockTicks = 100, Counter = 0 },
            Site = ProcessSite.OhioForge,
            OperatorId = "operator:test",
            Description = "forge",
            ProcessStepCompleted = new ProcessStepCompleted { Stage = ProcessStage.Forge },
        });

        var view = await compliance.GetPartComplianceAsync(new GetPartComplianceRequest { Serial = serial });

        Assert.That(view.BaselineState, Is.EqualTo(ComplianceState.Nominal));
        Assert.That(view.LatticeState, Is.EqualTo(ComplianceState.Nominal));
        Assert.That(view.Diverges, Is.False);
    }

    [Test]
    public async Task EmitFactStream_counts_all_accepted()
    {
        using var channel = _fixture.CreateChannel();
        var client = new FactIngressService.FactIngressServiceClient(channel);

        using var call = client.EmitFactStream();
        for (var i = 0; i < 3; i++)
        {
            await call.RequestStream.WriteAsync(new FactEnvelope
            {
                FactId = Guid.NewGuid().ToString(),
                Serial = $"HPT-BLD-S1-2028-{i:D5}",
                Hlc = new Hlc { WallClockTicks = i + 1, Counter = 0 },
                Site = ProcessSite.OhioForge,
                OperatorId = "operator:test",
                Description = "forge",
                ProcessStepCompleted = new ProcessStepCompleted { Stage = ProcessStage.Forge },
            });
        }
        await call.RequestStream.CompleteAsync();
        var summary = await call;

        Assert.That(summary.AcceptedCount, Is.EqualTo(3));
    }
}
