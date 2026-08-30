using System.IO;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Hygiene;

/// <summary>
/// The Explorer decides unencrypted-HTTP/2 transport per channel, never per
/// process. This gate holds that line at the source level (issue #1784).
/// </summary>
/// <remarks>
/// <para>
/// Seven gRPC client factories used to call
/// <c>AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)</c>
/// from inside a <em>per-circuit</em> channel factory, gated on that circuit's
/// endpoint having opted into unencrypted transport. The switch is
/// process-global and effectively write-once, so one circuit connecting to an
/// <c>http://</c> endpoint decided the posture for every subsequent channel in
/// the process - including circuits that never opted in. On the Blazor Server
/// head, where circuits are per-browser and share a process, that is one
/// operator's choice leaking onto every other operator.
/// </para>
/// <para>
/// The pattern was self-replicating: each new gRPC client was written by copying
/// an established sibling, so the epic that added the Telemetry and Tenancy
/// clients inherited it twice over. Channel construction now lives once in
/// <see cref="LatticeGrpcChannelFactory"/>, which scopes the transport handler to
/// the single channel that asked for it, and this fixture is what stops the next
/// client reintroducing the switch by copying a sibling again.
/// </para>
/// <para>
/// A source scan rather than a reflection walk, because the point is that the
/// call is never <em>written</em>: nothing in the type system stops any assembly
/// from mutating process-global state, so the prohibition has to be checked
/// against the source.
/// </para>
/// </remarks>
[TestFixture]
public sealed class AppContextSwitchHygieneTests
{
    private const string ExplorerSourceRoot = "src/lattice.explorer";

    /// <summary>
    /// The forms a reintroduction would take. The first is the call itself; the
    /// second is the switch name, so a reintroduction that reached the same
    /// process-global posture by another route (a runtime-setting helper, a
    /// constant lifted to a field) is caught as well.
    /// </summary>
    private static readonly string[] ProhibitedForms =
    [
        "AppContext.SetSwitch(",
        "Http2UnencryptedSupport",
    ];

    [Test]
    public void No_explorer_source_mutates_process_global_app_context_state()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var root = Path.Combine(repoRoot, ExplorerSourceRoot.Replace('/', Path.DirectorySeparatorChar));

        var violations = new List<string>();
        var scanned = 0;

        foreach (var pattern in new[] { "*.cs", "*.razor" })
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

        // Without this the gate would pass vacuously if the Explorer moved.
        Assert.That(scanned, Is.GreaterThan(1), "the scan must reach the Explorer's sources");

        Assert.That(violations, Is.Empty,
            "The Explorer configures unencrypted HTTP/2 per channel, not per process. AppContext "
            + "state is global and effectively write-once, so a per-circuit opt-in written there "
            + "decides the transport posture for every other circuit sharing the process. Build the "
            + $"channel through {nameof(LatticeGrpcChannelFactory)} instead, which scopes the "
            + "transport handler to the one channel that asked for it."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_scanner_detects_a_call_it_is_shown()
    {
        // Battery test for the smoke detector: a change that neutered the match
        // would let the gate above pass on an Explorer that had grown a
        // violation. Prose - including this fixture's own rationale, which lives
        // in the factory's XML docs - must not trip it.
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
    /// named in prose - the factory documents exactly why it must not be called -
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
