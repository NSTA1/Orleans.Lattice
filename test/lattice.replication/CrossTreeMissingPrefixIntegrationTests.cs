using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// A persistent missing-prefix marker requires automatic recovery to be off.
/// The default-true positive fixture may legitimately bootstrap and retire it.
/// GC stays enabled; only this separate fixture changes receiver recovery policy.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class CrossTreeMissingPrefixIntegrationTests
{
    private readonly CrossTreeSiblingBoundaryIntegrationTests _fixture = new();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.SetUpAsync(builder =>
        builder.AddSiloBuilderConfigurator<RecoveryDisabledSiteAConfigurator>());

    [SetUp]
    public void BeforeEach() => _fixture.BeforeEach();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.TearDown();

    [Test]
    public Task Reverse_reader_after_real_gc_requires_reseed_despite_converged_plain_writes() =>
        _fixture.AssertMissingPrefixRequiresReseedAsync();

    private sealed class RecoveryDisabledSiteAConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) =>
            siloBuilder.Services.PostConfigureAll<LatticeReplicationOptions>(options =>
                options.AutoBootstrapOnFallOffLog = false);
    }
}
