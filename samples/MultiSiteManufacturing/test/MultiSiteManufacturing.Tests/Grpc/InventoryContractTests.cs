using MultiSiteManufacturing.Contracts.V1;
using Grpc.Core;

namespace MultiSiteManufacturing.Tests.Grpc;

[TestFixture]
public sealed class InventoryContractTests
{
    private GrpcContractFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new GrpcContractFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    [Test]
    public async Task CreatePart_returns_formatted_serial()
    {
        using var channel = _fixture.CreateChannel();
        var client = new InventoryService.InventoryServiceClient(channel);

        var part = await client.CreatePartAsync(new CreatePartRequest
        {
            Family = "HPT-BLD-S1",
            OperatorId = "operator:test",
        });

        Assert.That(part.Family, Is.EqualTo("HPT-BLD-S1"));
        Assert.That(part.Serial, Does.StartWith("HPT-BLD-S1-"));
        Assert.That(part.Serial, Does.EndWith("-00001"));
    }

    [Test]
    public async Task ListParts_streams_every_created_part()
    {
        using var channel = _fixture.CreateChannel();
        var client = new InventoryService.InventoryServiceClient(channel);

        await client.CreatePartAsync(new CreatePartRequest { Family = "HPT-BLD-S1", OperatorId = "op" });
        await client.CreatePartAsync(new CreatePartRequest { Family = "HPT-BLD-S1", OperatorId = "op" });

        using var call = client.ListParts(new ListPartsRequest());
        var summaries = new List<PartSummary>();
        await foreach (var summary in call.ResponseStream.ReadAllAsync())
        {
            summaries.Add(summary);
        }

        Assert.That(summaries, Has.Count.EqualTo(2));
        Assert.That(summaries.Select(s => s.Family), Is.All.EqualTo("HPT-BLD-S1"));
        Assert.That(summaries, Is.All.Matches<PartSummary>(s => s.LatestStage == ProcessStage.Forge));
    }

    [Test]
    public async Task GetPart_returns_view_with_fact_trail()
    {
        using var channel = _fixture.CreateChannel();
        var client = new InventoryService.InventoryServiceClient(channel);

        var part = await client.CreatePartAsync(new CreatePartRequest { Family = "HPT-BLD-S1" });

        var view = await client.GetPartAsync(new GetPartRequest { Serial = part.Serial });

        Assert.That(view.Serial, Is.EqualTo(part.Serial));
        Assert.That(view.Family, Is.EqualTo("HPT-BLD-S1"));
        Assert.That(view.Facts, Has.Count.EqualTo(1));
        Assert.That(view.Facts[0].Kind, Is.EqualTo("ProcessStepCompleted"));
        Assert.That(view.BaselineState, Is.EqualTo(ComplianceState.Nominal));
        Assert.That(view.LatticeState, Is.EqualTo(ComplianceState.Nominal));
    }
}
