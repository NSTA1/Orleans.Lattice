using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Repository guard for issue #2423: every background entry point in the
/// repo-context surface must either establish a credential before it reads the
/// lattice, or be listed here as a deliberate, justified exclusion.
/// </summary>
/// <remarks>
/// <para>
/// A background turn starts with no ambient caller credential. Under a
/// fail-closed access gate its reads are therefore denied - and a denied
/// <b>range</b> read does not throw. It resolves to a reject-all key filter and
/// returns a clean, successful, empty result, so the component concludes the
/// store is empty and carries on with a healthy log at every layer. That single
/// mechanism produced issues #2277, #2252/#2407, #2406 and #2480.
/// </para>
/// <para>
/// <b>Why this guard keys on the entry point rather than on the read.</b> The
/// obvious guard - "find the components that call a range-read verb" - is
/// unsound here, and the audit for #2423 demonstrated it rather than assuming
/// it: a scan of every background component for a range-read verb matched one
/// file, and did <em>not</em> match either of the two components whose defects
/// were already confirmed (#2406, #2426). Every real instance reads
/// <em>indirectly</em>, through <c>RepoContextStore.ListRepoIdsAsync</c>, a
/// vector source, or a gap scanner, so the read verb is arbitrarily many call
/// levels away from the component and no per-file pattern can see it. Keying on
/// the entry point instead is what makes the guard sound, because the entry
/// point is where the credential is absent and is the only place it can be
/// established.
/// </para>
/// <para>
/// <b>Why "is a hosted service" would be the wrong property.</b> Issue #2406's
/// own body eliminated its true cause by reasoning about which components were
/// hosted services, when the operative property was which components carried a
/// credential. An elimination is exactly as strong as the property it eliminates
/// on, so this guard tests for the credential itself.
/// </para>
/// </remarks>
[TestFixture]
public sealed class BackgroundCredentialScopeGuardTests
{
    /// <summary>
    /// The marker that establishes a caller credential for a background turn.
    /// Both <c>Use</c> and <c>With</c> are scoping entry points on it.
    /// </summary>
    private const string CredentialMarker = "LatticeCredentialContext";

    /// <summary>
    /// Background entry points that legitimately establish no credential, each
    /// with the reason it needs none. A component belongs here only when it
    /// performs <b>no</b> lattice access on its background path; if it reads,
    /// it must carry a credential instead.
    /// <para>
    /// Keep the reasons: an undocumented exclusion is how a later reader
    /// concludes this guard covered a component it deliberately skipped.
    /// </para>
    /// </summary>
    private static readonly Dictionary<string, string> CredentialFreeBackgroundComponents = new(StringComparer.Ordinal)
    {
        ["RepoContextIndexingCadenceReporter.cs"] =
            "Logs the resolved indexing cadence options at startup and returns a completed task. "
            + "It has no lattice reference and performs no await, so it has no gated read to authorize.",
        ["RepoContextEffectiveConfigurationReporter.cs"] =
            "Reports the host's effective configuration (environment variables and options) at startup. "
            + "It reads configuration only and never touches a tree.",
        ["RepoIndexJobGrain.cs"] =
            "Its resume reminder only re-enqueues the request onto RepoIndexRunner and writes its own grain "
            + "state. The runner stamps the fixed run credential at its own drain, which is the single point "
            + "every run funnels through, so the credential is established there rather than here.",
    };

    private static readonly string[] ScanRoots =
    {
        Path.Combine("src", "lattice.api.mcp.repocontext"),
        Path.Combine("apps", "repocontext"),
    };

    // A background turn origin: a hosted service, or a grain reminder callback.
    // Both begin a turn that carries no caller credential.
    private static readonly Regex BackgroundEntryPoint = new(
        @":\s*(I[A-Za-z]*\s*,\s*)*(IHostedService|BackgroundService)\b|Task\s+ReceiveReminder\s*\(",
        RegexOptions.Compiled);

    [Test]
    public void Every_background_entry_point_establishes_a_credential_or_is_a_documented_exclusion()
    {
        var components = FindBackgroundComponents();

        // The guard must not go vacuous. If the scan stops matching - a moved
        // directory, a renamed base type - a silently empty set would report
        // green forever while covering nothing.
        Assert.That(
            components,
            Is.Not.Empty,
            "The background-component scan matched nothing, so this guard is not testing anything. "
            + "Fix the scan roots or the entry-point pattern rather than accepting the green.");

        var uncovered = new List<string>();
        foreach (var (fileName, text) in components)
        {
            if (text.Contains(CredentialMarker, StringComparison.Ordinal)) continue;
            if (CredentialFreeBackgroundComponents.ContainsKey(fileName)) continue;
            uncovered.Add(fileName);
        }

        Assert.That(
            uncovered,
            Is.Empty,
            $"These background components start an uncredentialed turn and are not documented exclusions: "
            + $"{string.Join(", ", uncovered)}. Under a fail-closed access gate their range reads return "
            + "EMPTY rather than throwing, so the component silently concludes the store is empty. Either "
            + $"establish a credential with {CredentialMarker}, or add the component to "
            + $"{nameof(CredentialFreeBackgroundComponents)} with the reason it needs none.");
    }

    [Test]
    public void Every_documented_exclusion_still_names_a_real_background_component()
    {
        // Stops the exclusion list rotting into a set of stale names that quietly
        // excuse nothing, and stops a renamed component carrying its exemption
        // with it by accident.
        var components = FindBackgroundComponents()
            .Select(static c => c.FileName)
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(components, Is.Not.Empty, "The background-component scan matched nothing.");

        var stale = CredentialFreeBackgroundComponents.Keys
            .Where(name => !components.Contains(name))
            .ToList();

        Assert.That(
            stale,
            Is.Empty,
            $"These names are excluded but are no longer background components: {string.Join(", ", stale)}. "
            + "Remove them so the list keeps describing the code as it is.");
    }

    [Test]
    public void Every_documented_exclusion_carries_a_reason()
    {
        var blank = CredentialFreeBackgroundComponents
            .Where(static kv => string.IsNullOrWhiteSpace(kv.Value))
            .Select(static kv => kv.Key)
            .ToList();

        Assert.That(
            blank,
            Is.Empty,
            $"These exclusions carry no reason: {string.Join(", ", blank)}. An undocumented exclusion is "
            + "indistinguishable from coverage to the next reader.");
    }

    private static List<(string FileName, string Text)> FindBackgroundComponents()
    {
        var root = HygieneRepository.FindRepoRoot();
        var found = new List<(string, string)>();

        foreach (var relative in ScanRoots)
        {
            var dir = Path.Combine(root, relative);
            if (!Directory.Exists(dir)) continue;

            foreach (var path in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
            {
                if (path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                var text = File.ReadAllText(path);
                if (BackgroundEntryPoint.IsMatch(text))
                {
                    found.Add((Path.GetFileName(path), text));
                }
            }
        }

        return found;
    }
}
