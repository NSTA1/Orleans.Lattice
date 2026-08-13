namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Tests for <see cref="RepoContextIndexingOptions.FromEnvironment"/>: it reads the
/// background-indexing cadence from <c>LATTICE_*</c> environment variables and falls back
/// to the behaviour-preserving defaults for any variable that is absent or malformed.
/// </summary>
[TestFixture]
public sealed class RepoContextIndexingOptionsTests
{
    private static readonly string[] AllKeys =
    [
        RepoContextIndexingOptions.TickIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileJitterSecondsKey,
        RepoContextIndexingOptions.FullWalkIntervalSecondsKey,
    ];

    [SetUp]
    [TearDown]
    public void ClearEnvironment()
    {
        foreach (var key in AllKeys)
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Test]
    public void FromEnvironment_uses_the_defaults_when_no_variables_are_set()
    {
        var options = RepoContextIndexingOptions.FromEnvironment();

        var defaults = new RepoContextIndexingOptions();
        Assert.Multiple(() =>
        {
            Assert.That(options.TickInterval, Is.EqualTo(defaults.TickInterval));
            Assert.That(options.ReconcileInterval, Is.EqualTo(defaults.ReconcileInterval));
            Assert.That(options.ReconcileIntervalJitter, Is.EqualTo(defaults.ReconcileIntervalJitter));
            Assert.That(options.FullWalkInterval, Is.EqualTo(defaults.FullWalkInterval));
        });
    }

    [Test]
    public void FromEnvironment_reads_each_variable_in_seconds()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TickIntervalSecondsKey, "5");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "5");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileJitterSecondsKey, "0");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.FullWalkIntervalSecondsKey, "120");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.TickInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.ReconcileInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.ReconcileIntervalJitter, Is.EqualTo(TimeSpan.Zero));
            Assert.That(options.FullWalkInterval, Is.EqualTo(TimeSpan.FromSeconds(120)));
        });
    }

    [Test]
    public void FromEnvironment_falls_back_to_the_default_for_a_malformed_value()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "not-a-number");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.ReconcileInterval, Is.EqualTo(new RepoContextIndexingOptions().ReconcileInterval));
    }

    [Test]
    public void FromEnvironment_falls_back_to_the_default_for_a_negative_value()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TickIntervalSecondsKey, "-3");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.TickInterval, Is.EqualTo(new RepoContextIndexingOptions().TickInterval));
    }
}
