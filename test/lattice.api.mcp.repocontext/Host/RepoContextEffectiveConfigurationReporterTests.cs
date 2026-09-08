using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the startup effective-configuration report (issue #2294): that the
/// resolved value is stated and the overridden subset marked, that credential-bearing
/// values cannot reach the log, and that the report cannot silently stop covering a
/// setting somebody adds later.
/// </summary>
/// <remarks>
/// <para>
/// The defect these cover is not a wrong setting. It is that the deployment's real
/// settings arrive from an untracked compose override, so no tracked file states what the
/// container runs and the divergence left no trace. The report is the trace.
/// </para>
/// <para>
/// Two tests here are load-bearing in a way the rest are not.
/// <see cref="An_unclassified_key_is_redacted_rather_than_printed"/> pins the allowlist
/// direction: inverted to a denylist it still passes every other test in this file while
/// leaking the next credential-bearing key somebody adds.
/// <see cref="Every_LATTICE_key_this_host_declares_is_covered_by_the_report"/> pins the
/// report against the drift that produced #2294 in the first place - a claim about
/// configuration that nothing checks.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextEffectiveConfigurationReporterTests
{
    private const string UnknownKey = "LATTICE_SOMETHING_NOBODY_HAS_CLASSIFIED";

    // Supplied value and assertion reference one symbol deliberately. Two independent
    // literals could drift apart, and an assertion checking for a token the test never
    // supplied would still pass while checking nothing - which is the failure this wave
    // keeps cataloguing. Deliberately not credential-shaped, because a redaction layer on
    // a viewing path rewrites a credential-shaped literal, and a reader shown the rewrite
    // sees exactly that vacuous test whether or not one is present.
    private const string SuppliedSecret = "sentinel-2294-must-not-be-logged";

    private static IConfiguration Configuration(params (string Key, string Value)[] settings)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(settings.ToDictionary(s => s.Key, s => (string?)s.Value))
            .Build();

    [Test]
    public void An_unclassified_key_is_redacted_rather_than_printed()
        => Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEffectiveConfiguration.IsSafeToPrint(UnknownKey),
                Is.False,
                "a key nobody has classified must not be assumed harmless");
            Assert.That(
                RepoContextEffectiveConfiguration.RenderValue(UnknownKey, SuppliedSecret),
                Is.EqualTo(RepoContextEffectiveConfiguration.UnclassifiedMarker),
                "the allowlist is chosen on which failure announces itself: an unclassified "
                + "value printed in full leaks silently into logs this wave has pasted into "
                + "issues all day, whereas a redacted value that was safe is visibly wrong "
                + "to the first reader and gets classified in the next commit");
        });

    [TestCase("LATTICE_POSTGRES_CONNECTION_STRING")]
    [TestCase("LATTICE_AZURE_STORAGE_CONNECTION_STRING")]
    public void A_credential_bearing_key_is_not_on_the_allowlist(string key)
        => Assert.That(
            RepoContextEffectiveConfiguration.IsSafeToPrint(key),
            Is.False,
            "these are the two keys in this host that carry a credential; adding either to "
            + "the allowlist would write it to output that is routinely pulled with docker "
            + "logs, which would be a worse defect than the unreviewable configuration this "
            + "report exists to fix");

    [Test]
    public void A_supplied_credential_is_reported_as_present_without_disclosing_it()
    {
        var line = RepoContextEffectiveConfiguration.DescribeSetting(
            "LATTICE_POSTGRES_CONNECTION_STRING",
            SuppliedSecret,
            null);

        Assert.Multiple(() =>
        {
            Assert.That(
                line,
                Does.Not.Contain(SuppliedSecret),
                "the value must never appear, in either position of the line");
            Assert.That(
                line,
                Does.Contain("[OVERRIDDEN"),
                "whether a credential was supplied at all is not itself a secret, and it is "
                + "the part an operator diagnosing a durability profile actually needs");
        });
    }

    [Test]
    public void A_classified_setting_states_the_value_the_process_resolved()
        => Assert.That(
            RepoContextEffectiveConfiguration.DescribeSetting(
                RepoContextHostConfiguration.McpPortKey, "8080", "8080"),
            Is.EqualTo("LATTICE_MCP_PORT = 8080"),
            "an un-overridden setting carries no marker, so the overridden ones stand out");

    [Test]
    public void An_overridden_setting_is_marked_and_carries_the_default_it_departed_from()
        => Assert.That(
            RepoContextEffectiveConfiguration.DescribeSetting(
                RepoContextHostConfiguration.DataRootKey, "/mnt/data", "/data"),
            Is.EqualTo("LATTICE_DATA_ROOT = /mnt/data [OVERRIDDEN, default /data]"),
            "this is the evidence shape #2294 was actually proved with - a resolved value "
            + "read against an expected one - and the marker is what makes the overridden "
            + "subset greppable instead of a twenty-line eyeball diff");

    [Test]
    public void A_variable_supplied_but_read_by_nothing_is_reported()
    {
        var lines = RepoContextEffectiveConfiguration.DescribeUnreadVariables(
            [new KeyValuePair<string, string?>(UnknownKey, "3")],
            RepoContextEffectiveConfigurationReporter.KnownKeys);

        Assert.That(
            lines,
            Has.Exactly(1).Contains("[SUPPLIED BUT NOT READ BY THIS HOST]"),
            "this is the failure issue #2279 actually suffered: the host binds every "
            + "LATTICE_ setting by hand, so a compose file could name a knob nothing reads "
            + "and there was no signal separating 'applied' from 'ignored'");
    }

    [Test]
    public void A_variable_the_host_does_read_is_not_reported_as_unread()
        => Assert.That(
            RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                [new KeyValuePair<string, string?>(RepoContextHostConfiguration.McpPortKey, "9000")],
                RepoContextEffectiveConfigurationReporter.KnownKeys),
            Is.Empty,
            "a false positive here would train readers to ignore the warning, which costs "
            + "more than the warning is worth");

    [Test]
    public void A_variable_outside_the_LATTICE_prefix_is_not_reported_as_unread()
        => Assert.That(
            RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                [new KeyValuePair<string, string?>("PATH", "/usr/bin")],
                RepoContextEffectiveConfigurationReporter.KnownKeys),
            Is.Empty,
            "the report is a configuration statement, not an environment dump");

    [Test]
    public void Every_LATTICE_key_this_host_declares_is_covered_by_the_report()
    {
        var declared = typeof(RepoContextHostConfiguration).Assembly
            .GetTypes()
            .Where(t => t.Namespace == typeof(RepoContextHostConfiguration).Namespace)
            .SelectMany(t => t.GetFields(BindingFlags.Public | BindingFlags.Static))
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string?)f.GetRawConstantValue())
            .Where(v => v is not null
                && v.StartsWith(RepoContextEffectiveConfiguration.LatticePrefix, StringComparison.Ordinal)
                && v.Length > RepoContextEffectiveConfiguration.LatticePrefix.Length)
            .Select(v => v!)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                declared,
                Is.Not.Empty,
                "the scan must find the host's key constants; a scan that matched nothing "
                + "would pass vacuously and guard nothing at all");
            Assert.That(
                declared.Except(RepoContextEffectiveConfigurationReporter.KnownKeys, StringComparer.OrdinalIgnoreCase),
                Is.Empty,
                "a key this host reads but the report does not list would be reported as "
                + "'supplied but not read', which is exactly backwards. This test is the "
                + "reason the report cannot become the stale claim about configuration that "
                + "#2294 was filed about");
        });
    }

    [Test]
    public void Every_covered_key_is_classified_one_way_or_the_other_and_the_split_is_pinned()
    {
        var redacted = RepoContextEffectiveConfigurationReporter.KnownKeys
            .Where(k => !RepoContextEffectiveConfiguration.IsSafeToPrint(k))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            redacted,
            Is.EqualTo(new[]
            {
                RepoContextHostConfiguration.AzureConnectionKey,
                RepoContextHostConfiguration.PostgresConnectionKey,
            }),
            "the redacted set is pinned so that widening it is a deliberate, reviewable "
            + "edit rather than a side effect; if a new key belongs here, this assertion is "
            + "where that decision gets recorded");
    }

    [Test]
    public void The_reporter_states_an_override_end_to_end()
    {
        var configuration = Configuration((RepoContextHostConfiguration.DataRootKey, "/mnt/data"));
        var logger = new CapturingLogger();
        var reporter = new RepoContextEffectiveConfigurationReporter(
            RepoContextHostConfiguration.FromConfiguration(configuration), configuration, logger);

        reporter.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        Assert.That(
            logger.Messages,
            Has.Exactly(1).Contains("LATTICE_DATA_ROOT = /mnt/data [OVERRIDDEN, default /data]"),
            "the default is derived by resolving the same configuration class against an "
            + "empty configuration, so it cannot drift from the code that applies it");
    }

    [Test]
    public void The_reporter_never_fails_startup()
    {
        var configuration = Configuration();
        var reporter = new RepoContextEffectiveConfigurationReporter(
            RepoContextHostConfiguration.FromConfiguration(configuration),
            configuration,
            new CapturingLogger());

        Assert.That(
            async () =>
            {
                await reporter.StartAsync(CancellationToken.None);
                await reporter.StopAsync(CancellationToken.None);
            },
            Throws.Nothing,
            "this is observability only: a configuration the host disagrees with is still a "
            + "configuration the operator is entitled to run, so the reporter must never be "
            + "able to keep the process from starting");
    }

    [Test]
    public void A_malformed_knob_is_reported_rather_than_thrown()
    {
        var configuration = Configuration(
            (RepoContextReplayConcurrency.MaxConcurrentReplaysKey, "not-a-number"));
        var logger = new CapturingLogger();
        var reporter = new RepoContextEffectiveConfigurationReporter(
            RepoContextHostConfiguration.FromConfiguration(configuration), configuration, logger);

        reporter.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        Assert.That(
            logger.Messages,
            Has.Exactly(1).Contains("LATTICE_WAL_MAX_CONCURRENT_REPLAYS = <invalid:"),
            "the host's own call site owns rejecting this; the reporter reports it and "
            + "stands aside, because a reporter that could fail startup would be a worse "
            + "defect than the invisibility it removes");
    }

    private sealed class CapturingLogger : ILogger<RepoContextEffectiveConfigurationReporter>
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);
            Messages.Add(formatter(state, exception));
        }
    }
}
