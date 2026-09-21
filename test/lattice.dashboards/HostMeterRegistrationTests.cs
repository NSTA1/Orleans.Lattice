using System.IO;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Holds the line that every reference-architecture silo which exports metrics
/// over Prometheus registers the two runtime meters alongside the lattice meter
/// family (issue #2724).
/// </summary>
/// <remarks>
/// <para>
/// A meter that is never registered with the <c>MeterProvider</c> is discarded
/// at the listener, so its instruments are declared, firing, and wholly absent
/// from <c>/metrics</c>. That endpoint is byte-identical to one where the
/// instrument was never declared at all, and the source of the instrument is
/// byte-identical to a healthy one, so the omission is invisible both to a
/// HELP/TYPE-line test against the endpoint and to source inspection of the
/// instrument. It is only visible at the registration site, which is what this
/// gate scans.
/// </para>
/// <para>
/// The omission cost the repository a wrong survey conclusion: with only the
/// <c>orleans.lattice*</c> family registered, a live silo exported no heap,
/// GC-heap-size, working-set, or process-memory series and no grain-activation
/// latency, so an out-of-memory restart loop had to be diagnosed by inferring
/// heap composition from resident activation counts, and activation cost had to
/// be proxied by a grain-call duration that cannot separate activation from
/// call. Both signals existed the whole time.
/// </para>
/// <para>
/// A source scan rather than a runtime assertion, because the reference
/// architecture hosts consume the released Orleans.Lattice NuGet packages rather
/// than the projects in <c>src/</c> - deliberately, so they exercise the library
/// exactly as a downstream deployment would - and so cannot be referenced from a
/// test assembly. The registration is a fact about what is written, which is
/// what a scan can check.
/// </para>
/// </remarks>
[TestFixture]
public sealed class HostMeterRegistrationTests
{
    /// <summary>
    /// Repo-root-relative roots holding hosts that are deployed and operated,
    /// as opposed to demonstrated. The samples are excluded on purpose: each is
    /// scoped to teach one lattice surface, and nobody diagnoses a restart loop
    /// on a sample. The benchmark silo is excluded because it already publishes
    /// runtime telemetry through <c>AddRuntimeInstrumentation()</c>, and because
    /// changing the series a cohort rig exports mid-campaign invalidates the
    /// comparison it exists to make.
    /// </summary>
    private static readonly string[] ScanRoots = ["reference-architecture"];

    /// <summary>
    /// The call that marks a file as a metrics-exporting host, and therefore as
    /// in scope for the required registrations below.
    /// </summary>
    private const string ExporterCall = "AddPrometheusExporter(";

    /// <summary>
    /// The registrations a metrics-exporting host must carry, each paired with
    /// what its absence costs. Matched as the <c>AddMeter</c> CALL form on a
    /// non-comment line, never as the bare meter name: the hosts name both
    /// meters in prose explaining why they are registered, so a bare-name match
    /// would be satisfied by the comment alone and would still pass after the
    /// call it guards had been deleted.
    /// </summary>
    private static readonly (string Form, string Meter, string Cost)[] RequiredRegistrations =
    [
        (
            "AddMeter(\"Microsoft.Orleans\")",
            "Microsoft.Orleans",
            "the Orleans runtime meter: grain activation counts and activation latency "
            + "(orleans-catalog-activation-latency), activation collection, grain directory, "
            + "scheduler, and messaging. Without it there is no direct measure of activation "
            + "cost, only a call-duration proxy that cannot separate activation from call."
        ),
        (
            "AddMeter(\"System.Runtime\")",
            "System.Runtime",
            "the .NET runtime meter, built in from .NET 9 and needing no package reference: "
            + "GC heap size and total allocation, GC pause time, process working set, "
            + "thread-pool depth, and lock contention. Without it the endpoint carries no heap "
            + "or process-memory series at all."
        ),
    ];

    /// <summary>
    /// The lattice family registration, which must stay a wildcard. Either
    /// spelling is accepted: the literal, or the interpolation over
    /// <c>LatticeMetrics.MeterName</c> that the reference architecture uses.
    /// </summary>
    private static readonly string[] LatticeFamilyForms =
    [
        "\"orleans.lattice*\"",
        "MeterName}*\"",
    ];

    /// <summary>
    /// Every metrics-exporting host under <see cref="ScanRoots"/> registers the
    /// lattice family and both runtime meters.
    /// </summary>
    [Test]
    public void Every_metrics_exporting_host_registers_the_runtime_meters()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var hosts = FindExportingHosts(repoRoot);

        // Without this the gate passes vacuously if the hosts move or are
        // renamed, which is the failure mode a registration gate is least able
        // to notice: nothing to scan reads exactly like nothing wrong.
        Assert.That(hosts, Is.Not.Empty,
            "no metrics-exporting host was found under " + string.Join(", ", ScanRoots)
            + " - the gate cannot pass vacuously, so either the scan roots are stale or the "
            + "exporter call was renamed");

        var violations = new List<string>();

        foreach (var (path, lines) in hosts)
        {
            var relative = Path.GetRelativePath(repoRoot, path).Replace('\\', '/');
            var code = CodeLines(lines);

            if (!LatticeFamilyForms.Any(form => code.Any(l => l.Contains("AddMeter(", StringComparison.Ordinal) && l.Contains(form, StringComparison.Ordinal))))
            {
                violations.Add($"{relative}: does not register the orleans.lattice* family as a wildcard. "
                    + "AddMeter matches a meter name exactly and does not cascade, so a bare "
                    + "\"orleans.lattice\" silently drops every sibling meter (replication, membership, "
                    + "auth, backup, scaling).");
            }

            foreach (var (form, meter, cost) in RequiredRegistrations)
            {
                if (!code.Any(l => l.Contains(form, StringComparison.Ordinal)))
                {
                    violations.Add($"{relative}: does not register the \"{meter}\" meter. Add .{form} - {cost}");
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "A host that exports metrics over Prometheus but does not register a meter discards "
            + "that meter's series at the listener. The resulting endpoint is byte-identical to one "
            + "where the instrument was never declared, so the omission is invisible to every other "
            + "check in this repository."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    /// <summary>
    /// Battery test for the smoke detector. Each required registration is
    /// removed from a synthetic host in turn, and the detector must report
    /// exactly that one - so a change that neutered the match, or that
    /// conflated one clause with another, cannot pass unnoticed.
    /// </summary>
    [Test]
    public void The_scanner_detects_each_registration_it_is_shown_missing()
    {
        string[] complete =
        [
            "builder.Services.AddOpenTelemetry()",
            "    .WithMetrics(metrics => metrics",
            "        .AddMeter($\"{LatticeMetrics.MeterName}*\")",
            "        .AddMeter(\"Microsoft.Orleans\")",
            "        .AddMeter(\"System.Runtime\")",
            "        .AddPrometheusExporter());",
        ];

        Assert.Multiple(() =>
        {
            Assert.That(MissingFrom(complete), Is.Empty, "a complete registration must not trip the gate");

            Assert.That(
                MissingFrom(complete.Where(l => !l.Contains("Microsoft.Orleans", StringComparison.Ordinal))),
                Is.EqualTo(new[] { "Microsoft.Orleans" }),
                "dropping the Orleans runtime meter must be reported, and only that");

            Assert.That(
                MissingFrom(complete.Where(l => !l.Contains("System.Runtime", StringComparison.Ordinal))),
                Is.EqualTo(new[] { "System.Runtime" }),
                "dropping the .NET runtime meter must be reported, and only that");

            Assert.That(
                MissingFrom(complete.Where(l => !l.Contains("MeterName}*", StringComparison.Ordinal))),
                Is.EqualTo(new[] { "orleans.lattice*" }),
                "dropping the lattice family wildcard must be reported, and only that");

            // The load-bearing one. Both hosts name both meters in prose right
            // above the call, so a gate that matched the bare meter name would
            // stay green after the call itself was deleted - which is the exact
            // shape of defect this fixture exists to catch, reproduced one level
            // up.
            var commentedOut = complete
                .Select(l => l.Contains("AddMeter(\"System.Runtime\")", StringComparison.Ordinal)
                    ? "        // .AddMeter(\"System.Runtime\") - registered below instead"
                    : l);
            Assert.That(MissingFrom(commentedOut), Is.EqualTo(new[] { "System.Runtime" }),
                "a registration that survives only as a comment must not satisfy the gate");

            // A bare (non-wildcard) lattice registration is the defect the
            // wildcard clause exists to prevent, so it must not satisfy it.
            var bareFamily = complete
                .Select(l => l.Contains("MeterName}*", StringComparison.Ordinal)
                    ? "        .AddMeter(\"orleans.lattice\")"
                    : l);
            Assert.That(MissingFrom(bareFamily), Is.EqualTo(new[] { "orleans.lattice*" }),
                "a bare orleans.lattice registration must not satisfy the wildcard clause");
        });
    }

    /// <summary>
    /// The names of the required registrations absent from
    /// <paramref name="hostLines"/>, in declaration order, using exactly the
    /// matching the gate above uses.
    /// </summary>
    private static string[] MissingFrom(IEnumerable<string> hostLines)
    {
        var code = CodeLines(hostLines.ToArray());
        var missing = new List<string>();

        if (!LatticeFamilyForms.Any(form => code.Any(l => l.Contains("AddMeter(", StringComparison.Ordinal) && l.Contains(form, StringComparison.Ordinal))))
        {
            missing.Add("orleans.lattice*");
        }

        foreach (var (form, meter, _) in RequiredRegistrations)
        {
            if (!code.Any(l => l.Contains(form, StringComparison.Ordinal)))
            {
                missing.Add(meter);
            }
        }

        return [.. missing];
    }

    /// <summary>
    /// Every <c>*.cs</c> file under the scan roots that calls the Prometheus
    /// exporter, paired with its lines. Comment-only lines are retained here and
    /// filtered at the point of matching.
    /// </summary>
    private static List<(string Path, string[] Lines)> FindExportingHosts(string repoRoot)
    {
        var hosts = new List<(string, string[])>();

        foreach (var relativeRoot in ScanRoots)
        {
            var root = Path.Combine(repoRoot, relativeRoot.Replace('/', Path.DirectorySeparatorChar));
            Assert.That(Directory.Exists(root), Is.True, $"scan root '{relativeRoot}' does not exist on disk");

            foreach (var file in HygieneRepository.EnumerateFiles(root, "*.cs"))
            {
                var lines = File.ReadAllLines(file);
                if (CodeLines(lines).Any(l => l.Contains(ExporterCall, StringComparison.Ordinal)))
                {
                    hosts.Add((file, lines));
                }
            }
        }

        return hosts;
    }

    /// <summary>
    /// The non-comment lines of a source file. A registration that survives only
    /// as prose does not satisfy this gate.
    /// </summary>
    private static string[] CodeLines(string[] lines) =>
        [.. lines.Where(l =>
        {
            var trimmed = l.TrimStart();
            return !trimmed.StartsWith("//", StringComparison.Ordinal)
                && !trimmed.StartsWith("*", StringComparison.Ordinal)
                && !trimmed.StartsWith("/*", StringComparison.Ordinal);
        })];
}
