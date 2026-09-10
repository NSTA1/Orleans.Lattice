using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// The two defects the report carried about its own coverage: it called live settings
/// unread (#2460), and it enumerated a subset without saying so, which makes an omission
/// indistinguishable from a value that was checked and found unset (#2470).
/// </summary>
public sealed partial class RepoContextEffectiveConfigurationReporterTests
{
    /// <summary>
    /// The five keys issue #2460 names, which the report labelled
    /// <c>[SUPPLIED BUT NOT READ BY THIS HOST]</c> while the package was reading every one
    /// of them and steering the runtime with them.
    /// </summary>
    private static readonly string[] IssueKeys =
    [
        RepoContextIndexingOptions.TickIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileJitterSecondsKey,
        RepoContextIndexingOptions.FullWalkIntervalSecondsKey,
        RepoContextIndexingOptions.EmbeddingGapScanIntervalSecondsKey,
    ];

    [Test]
    public void A_package_key_supplied_to_the_process_is_not_reported_as_unread()
    {
        var environment = IssueKeys
            .Select(key => new KeyValuePair<string, string?>(key, "60"))
            .ToList();

        Assert.That(
            RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                environment,
                RepoContextEffectiveConfigurationReporter.KnownKeys,
                RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes),
            Is.Empty,
            "these are the five keys issue #2460 names. The package reads every one of them "
            + "and the report called them inert, which is worse than saying nothing: an "
            + "operator who set a throttle deliberately was told it had no effect while it "
            + "was visibly steering the runtime");
    }

    [Test]
    public void Every_package_key_reaches_the_report_as_a_value_line()
    {
        var logger = new CapturingLogger();
        var reporter = Reporter(logger);

        reporter.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEnvironmentVariables.All,
                Is.Not.Empty,
                "a per-key loop over an empty set passes vacuously; this floor is what stops "
                + "the assertion below silently ceasing to check anything");

            foreach (var key in RepoContextEnvironmentVariables.All)
            {
                Assert.That(
                    logger.Messages,
                    Has.Some.Contains(key + " = "),
                    $"'{key}' is recognised as read, so it is no longer reported as unread. "
                    + "Without a value line it would vanish from the report altogether, "
                    + "which is strictly worse for an operator than the wrong label #2460 "
                    + "was filed about - a wrong label is at least visible");
            }
        });
    }

    [Test]
    public void A_package_default_is_stated_so_an_override_is_visible()
    {
        var logger = new CapturingLogger();

        Reporter(logger).StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        var line = logger.Messages.Single(m =>
            m.Contains(RepoContextIndexingOptions.ReconcileIntervalSecondsKey + " = ", StringComparison.Ordinal));

        Assert.That(
            line,
            Does.Contain("900s"),
            "the 15-minute default has to appear on the line either as the resolved value or "
            + "as the default an override departed from. The resolved half is the PARSED "
            + "value, not the raw string: every one of these settings falls back to its "
            + "default on a malformed value, so reporting the raw text would state a value "
            + "nothing applied");
    }

    [Test]
    public void A_per_repository_git_variable_is_not_a_false_positive()
    {
        var name = RepoContextEnvironmentVariables.GitSourceSettingPrefix + "MY_REPO_URL";
        var environment = new[] { new KeyValuePair<string, string?>(name, "https://example.invalid/x.git") };

        Assert.That(
            RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                environment,
                RepoContextEffectiveConfigurationReporter.KnownKeys,
                RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes),
            Is.Empty,
            "the repository id sits in the middle of the name, so no exact-match set can "
            + "ever cover this family. Reporting a correctly-supplied member as unread would "
            + "be #2460's defect one level down");
    }

    [Test]
    public void A_prefix_matched_variable_is_reported_as_matched_by_prefix_only()
    {
        var name = RepoContextEnvironmentVariables.GitSourceSettingPrefix + "MY_REPO_URL";
        var environment = new[] { new KeyValuePair<string, string?>(name, "https://example.invalid/x.git") };

        var lines = RepoContextEffectiveConfiguration.DescribePrefixMatchedVariables(
            environment,
            RepoContextEffectiveConfigurationReporter.KnownKeys,
            RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes);

        Assert.That(
            lines,
            Has.Exactly(1).Contains("NOT VERIFIED INDIVIDUALLY"),
            "prefix membership is a weaker claim than name-for-name recognition: a typo "
            + "inside a recognised family names a setting nothing binds, and is "
            + "indistinguishable from a correct member. Counting it as read silently would "
            + "trade one unsupported claim for another rather than fix either");
    }

    [Test]
    public void A_prefix_matched_value_is_never_printed()
    {
        var name = RepoContextEnvironmentVariables.GitSourceSettingPrefix + "MY_REPO_TOKEN";
        const string secret = "sentinel-2460-must-not-be-logged";
        var environment = new[] { new KeyValuePair<string, string?>(name, secret) };

        var lines = RepoContextEffectiveConfiguration.DescribePrefixMatchedVariables(
            environment,
            RepoContextEffectiveConfigurationReporter.KnownKeys,
            RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes);

        Assert.Multiple(() =>
        {
            Assert.That(
                lines, Has.Exactly(1).Contains(name),
                "the variable must reach the report, or this assertion passes for the wrong "
                + "reason - an absent line withholds nothing");
            Assert.That(
                lines, Has.None.Contains(secret),
                "a member of this family may be a personal access token, and the allowlist "
                + "that would otherwise decide cannot classify a name it has never seen. "
                + "This path therefore never consults it and never prints a value");
        });
    }

    [Test]
    public void A_bare_prefix_is_not_treated_as_a_setting()
    {
        var environment = new[]
        {
            new KeyValuePair<string, string?>(RepoContextEnvironmentVariables.GitSourceSettingPrefix, "x"),
        };

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEffectiveConfiguration.DescribePrefixMatchedVariables(
                    environment,
                    RepoContextEffectiveConfigurationReporter.KnownKeys,
                    RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes),
                Is.Empty,
                "a variable equal to the prefix names no setting, so admitting it would let "
                + "a bare prefix masquerade as a recognised one");

            Assert.That(
                RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                    environment,
                    RepoContextEffectiveConfigurationReporter.KnownKeys,
                    RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes),
                Is.Not.Empty,
                "and having been refused by the family, it must land in the unread arm "
                + "rather than falling through both and being reported nowhere");
        });
    }

    [Test]
    public void An_unrecognised_variable_is_still_reported_as_unread()
    {
        var environment = new[]
        {
            new KeyValuePair<string, string?>("LATTICE_NOBODY_READS_THIS", "1"),
        };

        Assert.That(
            RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                environment,
                RepoContextEffectiveConfigurationReporter.KnownKeys,
                RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes),
            Has.Exactly(1).Contains("SUPPLIED BUT NOT READ BY THIS HOST"),
            "widening what counts as read must not disarm the arm: #2279's failure was a "
            + "variable bound by nothing and reported by nothing, and that is still the "
            + "failure this arm exists to make loud");
    }

    [Test]
    public void The_report_states_the_scope_it_speaks_inside()
    {
        var logger = new CapturingLogger();

        Reporter(logger).StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        Assert.That(
            logger.Messages,
            Has.Exactly(1).Contains(RepoContextEffectiveConfiguration.ScopeStatement),
            "an enumeration that does not state its scope is read as exhaustive, so a "
            + "setting the report never had access to reads as a setting it checked and "
            + "found unset. That is #2470, and the omission is loudest for exactly the "
            + "settings nobody thought to look for");
    }

    /// <summary>
    /// The scope statement has to be specific enough to answer the question that produced
    /// the item, or it is decoration.
    /// </summary>
    [Test]
    public void The_scope_statement_names_the_channel_it_cannot_see()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEffectiveConfiguration.ScopeStatement,
                Does.Contain("ConfigureLattice"),
                "a scope statement that only says 'this list may be incomplete' leaves an "
                + "operator no better off. The reader needs to know WHICH channel is "
                + "invisible in order to go and look at it");

            Assert.That(
                RepoContextEffectiveConfiguration.ScopeStatement,
                Does.Contain("WalRetention"),
                "WalRetention is the worked example, and it earns its place: it is a live "
                + "setting with no environment variable anywhere in the repository, so it "
                + "can never appear here however many keys are added");

            Assert.That(
                RepoContextEffectiveConfiguration.ScopeStatement,
                Does.Contain("not a setting proven unset"),
                "this is the sentence that converts silence into a bounded claim; without "
                + "it the rest is a caveat a reader can miss");
        });
    }

    [Test]
    public void The_scope_line_is_greppable_from_the_log()
    {
        var logger = new CapturingLogger();

        Reporter(logger).StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        Assert.That(
            logger.Messages,
            Has.Some.Contains("SCOPE:"),
            "the report is read out of 'docker logs' rather than out of a debugger, so the "
            + "boundary has to be findable by the same grep that finds the settings");
    }

    [Test]
    public void The_header_states_how_many_settings_the_enumeration_covers()
    {
        var logger = new CapturingLogger();

        Reporter(logger).StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        Assert.That(
            logger.Messages,
            Has.Some.Contains("setting(s), of which"),
            "the count makes the enumeration's own size visible, so a report that silently "
            + "shrinks is observable rather than merely shorter");
    }

    private static RepoContextEffectiveConfigurationReporter Reporter(CapturingLogger logger)
    {
        var configuration = new ConfigurationBuilder().Build();
        return new RepoContextEffectiveConfigurationReporter(
            RepoContextHostConfiguration.FromConfiguration(configuration),
            configuration,
            logger);
    }
}
