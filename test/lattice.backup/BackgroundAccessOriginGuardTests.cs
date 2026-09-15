using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Repository guard for issue #2608: every background entry point in the backup
/// surface must either establish a system origin before it reads the lattice, or
/// be listed here as a deliberate, justified exclusion.
/// </summary>
/// <remarks>
/// <para>
/// A background turn starts with no ambient caller subject. On a host whose
/// access gate defaults to deny, its reads are therefore refused - and for a
/// scheduled backup the refusal is indistinguishable from having nothing to back
/// up. That is the same mechanism that produced #2423's family in the
/// repo-context surface: a measurand that is never exercised, whose silence
/// reads as good news.
/// </para>
/// <para>
/// <b>Why this guard keys on the entry point rather than on the read.</b> The
/// audit for #2423 demonstrated - rather than assumed - that scanning background
/// components for a read verb is unsound: it matched one file and missed both of
/// the components whose defects were already confirmed, because every real
/// instance reads indirectly, arbitrarily many call levels away. The entry point
/// is where the origin is absent and is the only place it can be established, so
/// that is the sound property to test.
/// </para>
/// <para>
/// <b>Why this is a separate fixture from the repo-context one.</b> The two scan
/// different roots and accept different markers: the repo-context guard requires
/// a caller credential (<c>LatticeCredentialContext</c>), while the backup
/// surface's infrastructure turns are system-authored and mark themselves with
/// <c>LatticeAccessGateContext.EnterSystemOrigin</c>. A single shared fixture
/// would have to accept either marker everywhere, weakening both.
/// </para>
/// </remarks>
[TestFixture]
public sealed class BackgroundAccessOriginGuardTests
{
    /// <summary>
    /// The marker that establishes an authorized origin for a background turn.
    /// <c>EnterSystemOrigin</c> is the scoping entry point on it.
    /// </summary>
    private const string OriginMarker = "LatticeAccessGateContext";

    /// <summary>
    /// Background entry points that legitimately establish no origin of their
    /// own, each with the reason it needs none. A component belongs here only
    /// when every lattice access on its background path is already scoped
    /// somewhere it can be pointed at; if it reads unscoped, it must carry the
    /// marker instead.
    /// <para>
    /// Keep the reasons: an undocumented exclusion is how a later reader
    /// concludes this guard covered a component it deliberately skipped.
    /// </para>
    /// </summary>
    private static readonly Dictionary<string, string> OriginFreeBackgroundComponents = new(StringComparer.Ordinal)
    {
        ["BackupHealthMonitorGrain.cs"] =
            "Its sweep reminder reaches the lattice only through LatticeBackupCatalogStore, "
            + "LatticeBackupHealthStore and InClusterLatticeBackupSink, each of which opens "
            + "EnterSystemOrigin at every single method because they hold backup infrastructure "
            + "metadata rather than tenant data. The origin is established one level down, at the "
            + "store, which is narrower than establishing it for the whole tick.",
        ["BackupHealthMonitorActivationService.cs"] =
            "Calls exactly one grain method (EnsureStartedAsync) to register the monitor's sweep "
            + "reminder, and otherwise only delays and logs. It holds no ILattice reference and "
            + "opens no tree, so it has no gated read to authorize; the grain it activates is "
            + "covered by its own entry above.",
        ["LatticeBackupReplicatedSinkStartupValidator.cs"] =
            "Validates at startup that a replicated host is not writing backups to a silo-local "
            + "sink. It consults ILatticeBackupSink (whose in-cluster implementation self-scopes at "
            + "every method) and IBackupSinkSharingProbe, a control-channel seam that queries peer "
            + "clusters rather than reading a tree. It opens no tree itself.",
    };

    private static readonly string[] ScanRoots = { Path.Combine("src", "lattice.backup") };

    // A background turn origin: a hosted service, or a grain reminder callback.
    // Both begin a turn that carries no caller subject.
    private static readonly Regex BackgroundEntryPoint = new(
        @":\s*(I[A-Za-z]*\s*,\s*)*(IHostedService|BackgroundService)\b|Task\s+ReceiveReminder\s*\(",
        RegexOptions.Compiled);

    [Test]
    public void Every_background_entry_point_establishes_an_origin_or_is_a_documented_exclusion()
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
            if (text.Contains(OriginMarker, StringComparison.Ordinal)) continue;
            if (OriginFreeBackgroundComponents.ContainsKey(fileName)) continue;
            uncovered.Add(fileName);
        }

        Assert.That(
            uncovered,
            Is.Empty,
            $"These background components start an unscoped turn and are not documented exclusions: "
            + $"{string.Join(", ", uncovered)}. On a host whose access gate defaults to deny their "
            + "reads are refused, and for a scheduled backup a refusal is indistinguishable from "
            + $"having nothing to back up. Either establish an origin with {OriginMarker}, or add the "
            + $"component to {nameof(OriginFreeBackgroundComponents)} with the reason it needs none.");
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

        var stale = OriginFreeBackgroundComponents.Keys
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
        var blank = OriginFreeBackgroundComponents
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
