using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Schema;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Data.Tests;

[TestFixture]
[Category("Integration")]
public sealed class SchemaValidationDataApiTests
{
    private TestCluster _cluster = null!;
    private ILatticeDataApi _api = null!;
    private IServiceProvider _services = null!;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _api = _services.GetRequiredService<ILatticeDataApi>();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task SetAsync_strict_json_rejection_preserves_typed_diagnostics_and_commits_nothing()
    {
        const string tree = "schema-data-validation";
        await _services.GetRequiredService<ILatticeSchemaAdmin>().SetPolicyAsync(
            tree, new LatticeSchemaPolicy([LatticeSchemaRule.Json()], strictIngest: true));

        await _api.SetAsync(tree, "valid", "{\"accepted\":true}"u8.ToArray());
        var fault = Assert.ThrowsAsync<LatticeSchemaViolationException>(
            () => _api.SetAsync(tree, "rejected", "secret-invalid-value"u8.ToArray()));
        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<ILatticeValueValidationFault>());
            Assert.That(fault!.Code, Is.EqualTo("schema-policy"));
            Assert.That(fault.TreeId, Is.EqualTo(tree));
            Assert.That(fault.Key, Is.EqualTo("rejected"));
            Assert.That(fault.Reason, Does.Contain("JSON"));
            Assert.That(fault.ToString(), Does.Not.Contain("secret-invalid-value"));
        });
        Assert.That((await _api.GetAsync(tree, "valid")).Value, Is.EqualTo("{\"accepted\":true}"u8.ToArray()));
        Assert.That((await _api.GetAsync(tree, "rejected")).Found, Is.False);

        var batch = new DataAtomicBatch
        {
            Upserts =
            [
                new DataEntry { Key = "atomic-valid", Value = "{}"u8.ToArray() },
                new DataEntry { Key = "atomic-invalid", Value = "secret-invalid-value"u8.ToArray() },
            ],
        };
        Assert.ThrowsAsync<LatticeSchemaViolationException>(
            () => _api.SetManyAtomicAsync(tree, batch, Guid.NewGuid().ToString("N")));
        Assert.That((await _api.GetAsync(tree, "atomic-valid")).Found, Is.False);
        Assert.That((await _api.GetAsync(tree, "atomic-invalid")).Found, Is.False);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeDataApi();
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
