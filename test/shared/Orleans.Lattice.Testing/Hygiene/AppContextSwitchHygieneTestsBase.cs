using System.IO;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Hygiene gate holding the line that no source in the scanned slice reaches
/// for process-global <see cref="AppContext"/> state to decide unencrypted
/// HTTP/2 transport (issues #1784, #1796).
/// </summary>
/// <remarks>
/// <para>
/// Two separate defects motivate the same prohibition. In the Explorer, seven
/// gRPC client factories called the switch from inside a <em>per-circuit</em>
/// channel factory: the switch is process-global and effectively write-once, so
/// one circuit connecting to an <c>http://</c> endpoint decided the posture for
/// every subsequent channel in the process, including circuits that never opted
/// in. In the samples and the reference architecture the same call was simply
/// inert - on .NET 10 h2c succeeds over an <c>http://</c> address whether the
/// switch is set, cleared, or never touched - so it was dead code that taught a
/// reader to reach for process-global state they do not need.
/// </para>
/// <para>
/// The pattern is self-replicating: each new gRPC client and each new sample is
/// written by copying an established sibling, which is how it spread to sixteen
/// call sites. This gate is what stops the next one reintroducing it.
/// </para>
/// <para>
/// A source scan rather than a reflection walk, because the point is that the
/// call is never <em>written</em>: nothing in the type system stops any assembly
/// from mutating process-global state, so the prohibition has to be checked
/// against the source. Comment-only lines are skipped, so a file may still
/// explain why the switch is not called.
/// </para>
/// </remarks>
public abstract class AppContextSwitchHygieneTestsBase
{
    /// <summary>
    /// The forms a reintroduction would take. The first is the call itself; the
    /// second is the switch name, so a reintroduction that reached the same
    /// process-global posture by another route (a runtime-setting helper, a
    /// constant lifted to a field) is caught as well. Assembled from fragments
    /// so this file is not self-flagged by a slice that happens to include it.
    /// </summary>
    private static readonly string[] ProhibitedForms =
    [
        "AppContext" + ".SetSwitch(",
        "Http2" + "UnencryptedSupport",
    ];

    /// <summary>The file patterns scanned for a reintroduced call.</summary>
    private static readonly string[] ScannedPatterns = ["*.cs", "*.razor"];

    /// <summary>
    /// The repo-root-relative directories this fixture scans, for example
    /// <c>src/lattice.explorer</c>.
    /// </summary>
    /// <remarks>
    /// Deliberately plain roots rather than a <see cref="HygieneScanScope"/>: that type
    /// is the ownership registry for the <em>content</em>-hygiene gates, which partition
    /// the repository so every file is scanned exactly once. This gate is an additive
    /// scan over hand-picked directories, so declaring it through
    /// <see cref="HygieneScanScope.ForSlice(string[])"/> would enrol those directories in
    /// that partition and exclude them from the core repo-level em-dash and mojibake
    /// scans, which is a coverage regression this gate has no business causing.
    /// </remarks>
    protected abstract IReadOnlyList<string> ScanRoots { get; }

    /// <summary>
    /// The slice-specific remediation appended to the failure message: what the
    /// author should write instead of the process-global switch.
    /// </summary>
    protected abstract string RemediationHint { get; }

    /// <summary>
    /// Scans the C# and Razor sources under <see cref="ScanRoots"/> and fails if the
    /// process-global unencrypted-HTTP/2 switch is set (or named outside a comment)
    /// anywhere.
    /// </summary>
    [Test]
    public void No_scanned_source_mutates_process_global_app_context_state()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var violations = new List<string>();
        var scanned = 0;

        foreach (var relativeRoot in ScanRoots)
        {
            var root = Path.Combine(repoRoot, relativeRoot.Replace('/', Path.DirectorySeparatorChar));
            Assert.That(Directory.Exists(root), Is.True, $"scan root '{relativeRoot}' does not exist on disk");

            foreach (var pattern in ScannedPatterns)
            {
                foreach (var file in HygieneRepository.EnumerateFiles(root, pattern))
                {
                    scanned++;
                    var lines = File.ReadAllLines(file);
                    for (var i = 0; i < lines.Length; i++)
                    {
                        if (IsComment(lines[i]))
                        {
                            continue;
                        }

                        foreach (var form in ProhibitedForms)
                        {
                            if (lines[i].Contains(form, StringComparison.Ordinal))
                            {
                                violations.Add($"{Relative(repoRoot, file)}:{i + 1}: {lines[i].Trim()}");

                                // One report per line: a single call matches both
                                // forms, and reporting it twice reads like two
                                // defects.
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Without this the gate would pass vacuously if the scanned tree moved.
        Assert.That(scanned, Is.GreaterThan(1), "the scan must reach the fixture's sources");

        Assert.That(violations, Is.Empty,
            "Unencrypted HTTP/2 is decided per channel, never per process. AppContext state is "
            + "global and effectively write-once, so an opt-in written there decides the transport "
            + "posture for every other channel sharing the process - and on .NET 10 this particular "
            + "switch does nothing at all, because grpc-dotnet speaks h2c by prior knowledge over an "
            + "http:// address regardless of it. "
            + RemediationHint
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    /// <summary>
    /// Battery test for the smoke detector: a change that neutered the match
    /// would let the gate above pass on a slice that had grown a violation.
    /// Prose - including the rationale that lives in XML docs - must not trip
    /// it.
    /// </summary>
    [Test]
    public void The_scanner_detects_a_call_it_is_shown()
    {
        const string CallSite =
            "        AppContext.SetSwitch(\"System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport\", true);";
        const string DocLine =
            "    /// switch, <c>Http2UnencryptedSupport</c>, is a .NET Core 3.x artefact.";

        Assert.Multiple(() =>
        {
            Assert.That(IsComment(CallSite), Is.False);
            Assert.That(
                ProhibitedForms.Any(form => CallSite.Contains(form, StringComparison.Ordinal)),
                Is.True,
                "a real call site must trip the gate");

            Assert.That(IsComment(DocLine), Is.True, "a doc line must be skipped as prose");
        });
    }

    /// <summary>
    /// Whether <paramref name="line"/> is comment-only, so the switch may be
    /// named in prose - a file may document exactly why it is not called -
    /// without tripping the gate.
    /// </summary>
    private static bool IsComment(string line)
    {
        var trimmed = line.TrimStart();
        return trimmed.StartsWith("//", StringComparison.Ordinal)
            || trimmed.StartsWith("*", StringComparison.Ordinal);
    }

    private static string Relative(string repoRoot, string file) =>
        Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
}
