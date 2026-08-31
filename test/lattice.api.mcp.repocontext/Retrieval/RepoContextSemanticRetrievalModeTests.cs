using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for the configuration decision D2: approximate semantic retrieval is the
/// default, and the exact brute-force scan remains available by configuration.
/// <para>
/// Both halves matter. The default must be the approximate plane, or the change
/// bounds nothing; and the exact path must still be selectable and still declare
/// complete recall, because it is the correctness oracle recall is measured
/// against and the escape hatch for a host that cannot accept bounded recall.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextSemanticRetrievalModeTests
{
    [SetUp]
    [TearDown]
    public void ClearEnvironment()
        => Environment.SetEnvironmentVariable(RepoContextIndexingOptions.SemanticRetrievalKey, null);

    private static ServiceProvider Build(RepoContextSemanticRetrievalMode? mode)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSerializer();
        services.AddSingleton(Substitute.For<IGrainFactory>());

        // Registered first so it wins the TryAdd inside AddRepoContextTools, which is
        // exactly how a host overrides the default.
        if (mode is not null)
        {
            services.AddSingleton(new RepoContextIndexingOptions { SemanticRetrieval = mode.Value });
        }

        services.AddRepoContextTools();
        return services.BuildServiceProvider();
    }

    [Test]
    public void The_default_is_the_approximate_plane()
    {
        using var provider = Build(mode: null);

        var index = provider.GetRequiredService<IRepoContextSemanticIndex>();

        Assert.Multiple(() =>
        {
            Assert.That(index, Is.TypeOf<AnnRepoContextSemanticIndex>(),
                "Approximate retrieval is the default, so the host binds it without being asked to.");
            Assert.That(index.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
                "And it declares the bounded-recall guarantee rather than the one the surface used to make.");
        });
    }

    [Test]
    public void Configuring_the_exact_mode_binds_the_brute_force_scan_unchanged()
    {
        using var provider = Build(RepoContextSemanticRetrievalMode.Exact);

        var index = provider.GetRequiredService<IRepoContextSemanticIndex>();

        Assert.Multiple(() =>
        {
            Assert.That(index, Is.TypeOf<ExactKnnSemanticIndex>(),
                "The exact path is not merely still present: it is what a host configured for exact gets.");
            Assert.That(index.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticExact),
                "It still declares complete recall, so today's contract is reproduced exactly.");
        });
    }

    [Test]
    public void Configuring_the_approximate_mode_explicitly_binds_the_plane()
    {
        using var provider = Build(RepoContextSemanticRetrievalMode.Approximate);

        Assert.That(
            provider.GetRequiredService<IRepoContextSemanticIndex>(),
            Is.TypeOf<AnnRepoContextSemanticIndex>());
    }

    [Test]
    public void The_exact_scan_stays_resolvable_as_the_oracle_even_when_the_plane_is_bound()
    {
        using var provider = Build(mode: null);

        var exact = provider.GetRequiredService<ExactKnnSemanticIndex>();

        Assert.That(exact.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticExact),
            "The exact scan is kept, not deleted: it answers while the index builds and is the recall oracle.");
    }

    [Test]
    public void The_environment_selects_the_mode_and_falls_back_on_anything_it_does_not_recognise()
    {
        Assert.Multiple(() =>
        {
            Environment.SetEnvironmentVariable(
                RepoContextIndexingOptions.SemanticRetrievalKey,
                RepoContextIndexingOptions.SemanticRetrievalExact);
            Assert.That(
                RepoContextIndexingOptions.FromEnvironment().SemanticRetrieval,
                Is.EqualTo(RepoContextSemanticRetrievalMode.Exact));

            Environment.SetEnvironmentVariable(
                RepoContextIndexingOptions.SemanticRetrievalKey, "  EXACT  ");
            Assert.That(
                RepoContextIndexingOptions.FromEnvironment().SemanticRetrieval,
                Is.EqualTo(RepoContextSemanticRetrievalMode.Exact),
                "Whitespace and case are tolerated; an operator should not have to guess the exact spelling.");

            Environment.SetEnvironmentVariable(
                RepoContextIndexingOptions.SemanticRetrievalKey,
                RepoContextIndexingOptions.SemanticRetrievalApproximate);
            Assert.That(
                RepoContextIndexingOptions.FromEnvironment().SemanticRetrieval,
                Is.EqualTo(RepoContextSemanticRetrievalMode.Approximate));

            Environment.SetEnvironmentVariable(RepoContextIndexingOptions.SemanticRetrievalKey, "exakt");
            Assert.That(
                RepoContextIndexingOptions.FromEnvironment().SemanticRetrieval,
                Is.EqualTo(RepoContextSemanticRetrievalMode.Approximate),
                "A typo falls back to the default rather than leaving the box on a path nobody chose.");

            Environment.SetEnvironmentVariable(RepoContextIndexingOptions.SemanticRetrievalKey, null);
            Assert.That(
                RepoContextIndexingOptions.FromEnvironment().SemanticRetrieval,
                Is.EqualTo(RepoContextSemanticRetrievalMode.Approximate),
                "An unset variable is the documented default.");
        });
    }
}
