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
public sealed partial class RepoContextEffectiveConfigurationReporterTests
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
        var declared = DeclaredLatticeConstants();

        Assert.Multiple(() =>
        {
            Assert.That(
                declared,
                Is.Not.Empty,
                "the scan must find the key constants; a scan that matched nothing "
                + "would pass vacuously and guard nothing at all");
            Assert.That(
                declared.Where(v => !Covered(v)),
                Is.Empty,
                "a key this host reads but the report does not list would be reported as "
                + "'supplied but not read', which is exactly backwards. This test is the "
                + "reason the report cannot become the stale claim about configuration that "
                + "#2294 was filed about");
        });
    }

    /// <summary>
    /// The scan's own reach, asserted rather than assumed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the assertion whose absence caused issue #2460. The guard above scanned only
    /// the host assembly's namespace while <see cref="RepoContextEffectiveConfigurationReporter.KnownKeys"/>
    /// claimed to cover every variable the host resolves - including the eleven the
    /// repository-context package resolves. The claim was broader than the check, the gap
    /// was invisible from either side, and the guard passed for years while every one of
    /// those keys was reported <c>[SUPPLIED BUT NOT READ BY THIS HOST]</c>.
    /// </para>
    /// <para>
    /// Widening the scan alone would not stop that recurring: a future key in a third
    /// assembly would be outside the widened scan exactly as it was outside the narrow one.
    /// Pinning the scanned assemblies makes the reach a reviewable decision, so adding an
    /// assembly the host reads configuration from fails here until somebody says so.
    /// </para>
    /// </remarks>
    [Test]
    public void The_coverage_scan_reaches_both_assemblies_that_declare_keys()
    {
        var scanned = ScannedAssemblies.Select(a => a.GetName().Name).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                scanned,
                Is.EquivalentTo(new[]
                {
                    "Orleans.Lattice.Api.Mcp.RepoContext.Host",
                    "Orleans.Lattice.Api.Mcp.RepoContext",
                }),
                "the scan's reach is pinned because a coverage guard is only as strong as "
                + "the surface it looks at, and #2460 is what a guard narrower than its own "
                + "claim costs");

            Assert.That(
                DeclaredLatticeConstants(),
                Has.Some.EqualTo(RepoContextIndexingOptions.ReconcileIntervalSecondsKey),
                "a positive control on the widened half: without it, a scan that silently "
                + "stopped reaching the package assembly would still pass the guard above");
        });
    }

    /// <summary>
    /// The assemblies the coverage scan reads key constants from: this host, and the
    /// repository-context package whose options classes the host composes.
    /// </summary>
    private static IReadOnlyList<Assembly> ScannedAssemblies { get; } =
    [
        typeof(RepoContextHostConfiguration).Assembly,
        typeof(RepoContextEnvironmentVariables).Assembly,
    ];

    /// <summary>
    /// Every <c>LATTICE_</c>-prefixed string constant declared by the scanned assemblies,
    /// which is the set the report must account for one way or another.
    /// </summary>
    private static IReadOnlyList<string> DeclaredLatticeConstants()
        => ScannedAssemblies
            .SelectMany(a => a.GetTypes())
            .SelectMany(t => t.GetFields(BindingFlags.Public | BindingFlags.Static))
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string?)f.GetRawConstantValue())
            .Where(v => v is not null
                && v.StartsWith(RepoContextEffectiveConfiguration.LatticePrefix, StringComparison.Ordinal)
                && v.Length > RepoContextEffectiveConfiguration.LatticePrefix.Length)
            .Select(v => v!)
            .Distinct(StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Whether the report accounts for a declared constant, either by naming it or by
    /// publishing it as a prefix under which a run-time-named family is read.
    /// </summary>
    private static bool Covered(string declared)
        => RepoContextEffectiveConfigurationReporter.KnownKeys
               .Contains(declared, StringComparer.OrdinalIgnoreCase)
           || RepoContextEffectiveConfigurationReporter.KnownKeyPrefixes
               .Contains(declared, StringComparer.OrdinalIgnoreCase);

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

    /// <summary>
    /// Covers <see cref="RepoContextEffectiveConfiguration.ReadProcessEnvironment"/>,
    /// the seam that supplies the real report its input. Every other test here hands
    /// <see cref="RepoContextEffectiveConfiguration.DescribeUnreadVariables"/> a
    /// hand-built list, so the whole "supplied but not read" arm of #2279 could work
    /// perfectly in this fixture while reading nothing at run time.
    /// <para>
    /// Asserted through the COMPOSITION rather than on the returned list, because that
    /// is the contract the method's own summary states - "the shape
    /// <c>DescribeUnreadVariables</c> accepts". A test that only asserted the pair is
    /// present would still pass if the two sides disagreed on prefix casing or on the
    /// null-value representation.
    /// </para>
    /// </summary>
    [Test]
    public void A_variable_set_on_this_process_reaches_the_unread_report()
    {
        const string probeKey = "LATTICE_PROBE_READ_PROCESS_ENVIRONMENT";
        Environment.SetEnvironmentVariable(probeKey, "1");
        try
        {
            var environment = RepoContextEffectiveConfiguration.ReadProcessEnvironment();

            Assert.That(
                environment.Any(pair => pair.Key is not null),
                Is.True,
                "the process environment always holds at least one variable; an empty read "
                + "here means the enumeration broke, not that the environment is empty - fix "
                + "the read rather than relaxing this floor");

            var lines = RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                environment,
                RepoContextEffectiveConfigurationReporter.KnownKeys);

            Assert.That(
                lines,
                Has.Exactly(1).Contains(probeKey),
                "the report is only a trace of the deployment if it reads the deployment's "
                + "actual environment; a variable set on this process and read by nothing "
                + "must arrive at the unread arm through the real seam");
        }
        finally
        {
            Environment.SetEnvironmentVariable(probeKey, null);
        }
    }

    /// <summary>
    /// The read drops non-string environment keys. Nothing downstream can render one,
    /// and a null name would otherwise be carried as far as the prefix test in
    /// <see cref="RepoContextEffectiveConfiguration.DescribeUnreadVariables"/>.
    /// </summary>
    [Test]
    public void The_process_environment_read_never_yields_a_null_name()
    {
        var names = RepoContextEffectiveConfiguration.ReadProcessEnvironment()
            .Select(pair => pair.Key)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                names, Is.Not.Empty,
                "an all-null assertion over an empty sequence is vacuously true, so this "
                + "floor is what stops the guard below silently ceasing to check anything. "
                + "If it trips, fix the read - do not delete the floor");

            Assert.That(
                names, Has.None.Null,
                "DescribeUnreadVariables guards against a null name, but this read is where "
                + "that guarantee is supposed to come from");
        });
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
