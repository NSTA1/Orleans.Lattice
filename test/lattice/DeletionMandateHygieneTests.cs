using System.IO;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Phase 6b hygiene gate for the universal cross-cluster atomic-visibility
/// primitive. The retired apply-mode receiver-side saga surface
/// (<c>ApplyManyAtomicAsync</c>, <c>ExecuteApplyAsync</c>,
/// <c>AtomicApplyEntry</c>, <c>AtomicApplyResult</c>,
/// <c>AtomicApplyOutcome</c>, <c>IsApplyMode</c> state slot) and the
/// retired staging-buffer scaffold (<c>IReplicationTxBufferGrain</c>,
/// <c>AtomicBatchDelivery</c> opt-in flag, <c>SnapshotSagaQuiesceTimeout</c>,
/// the <c>apply.batch.*</c> metric family) were superseded by the
/// universal source+receiver primitive built on per-tree
/// <c>ITxRegistryGrain</c> + per-leaf <c>_pendingTx</c> + per-shard
/// terminal mark + receiver wire seam (<c>ApplyPreparedSetAsync</c> /
/// <c>ApplyPreparedDeleteAsync</c> / <c>ApplyTxTerminalAsync</c>).
///
/// This test fails the build if any of the doomed identifiers reappear in
/// source or test code. Roadmap files are exempt because tracker entries
/// legitimately reference retired identifiers as part of the audit trail.
/// </summary>
[TestFixture]
public class DeletionMandateHygieneTests
{
    /// <summary>
    /// Identifiers that the universal-visibility ship explicitly retired.
    /// None may appear anywhere in <c>src/</c> or <c>test/</c> going
    /// forward; reintroducing any of them rolls back the deletion mandate
    /// and silently re-opens the partial-visibility window the new
    /// primitive closes.
    ///
    /// Each entry is assembled at runtime from fragments so this source
    /// file itself does not contain the literal identifier and is
    /// therefore not self-flagged. The fragmentation is purely cosmetic
    /// to keep this hygiene gate from becoming its own first violation;
    /// the runtime concatenation produces the exact identifier the gate
    /// scans for.
    /// </summary>
    private static readonly string[] DoomedIdentifiers =
    [
        "Atomic" + "ApplyEntry",
        "Atomic" + "ApplyResult",
        "Atomic" + "ApplyOutcome",
        "Apply" + "ManyAtomicAsync",
        "Execute" + "ApplyAsync",
        "Execute" + "ApplyStepAsync",
        "Is" + "ApplyMode",
        "IReplication" + "TxBufferGrain",
        "Replication" + "TxBufferGrain",
        "AtomicBatch" + "Delivery",
        "SnapshotSaga" + "QuiesceTimeout",
    ];

    /// <summary>
    /// Scans every <c>.cs</c> file under <c>src/</c> and <c>test/</c> and
    /// fails if any retired apply-mode / staging-buffer identifier is
    /// present. This test file is exempt (self-reference would otherwise
    /// trip the gate, even though the identifier strings here are
    /// fragment-assembled at runtime to keep the source bytes clean).
    /// </summary>
    [Test]
    public void Doomed_identifiers_have_no_remaining_references()
    {
        var repoRoot = FindRepoRoot();
        var thisFile = Path.GetFullPath(
            Path.Combine(repoRoot, "test", "lattice", "DeletionMandateHygieneTests.cs"));

        var scanned = new List<string>();
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, "src"), "*.cs"));
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, "test"), "*.cs"));

        var violations = new List<string>();
        foreach (var file in scanned)
        {
            var full = Path.GetFullPath(file);
            if (string.Equals(full, thisFile, StringComparison.OrdinalIgnoreCase)) continue;

            var lines = File.ReadAllLines(full);
            for (int i = 0; i < lines.Length; i++)
            {
                foreach (var doomed in DoomedIdentifiers)
                {
                    if (lines[i].Contains(doomed, StringComparison.Ordinal))
                    {
                        var rel = Path.GetRelativePath(repoRoot, full).Replace('\\', '/');
                        violations.Add($"{rel}:{i + 1}: '{doomed}' in: {lines[i].Trim()}");
                    }
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "Retired apply-mode / staging-buffer identifiers must not appear in source or test code. "
            + "These were superseded by the universal cross-cluster atomic-visibility primitive "
            + "(per-tree ITxRegistryGrain + per-leaf _pendingTx + per-shard terminal mark + "
            + "the prepared-Set / prepared-Delete / terminal-mark wire seam). "
            + "Reintroducing any of them silently re-opens the partial-visibility window the new "
            + "primitive closes." + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    private static IEnumerable<string> EnumerateFiles(string root, string pattern)
    {
        if (!Directory.Exists(root)) yield break;
        foreach (var file in Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories))
        {
            var parts = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (parts.Any(p => p.Equals("bin", StringComparison.OrdinalIgnoreCase)
                            || p.Equals("obj", StringComparison.OrdinalIgnoreCase)
                            || p.Equals("node_modules", StringComparison.OrdinalIgnoreCase)))
                continue;
            yield return file;
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "README.md"))
                && Directory.Exists(Path.Combine(dir.FullName, "docs"))
                && Directory.Exists(Path.Combine(dir.FullName, "src")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        throw new InvalidOperationException(
            "Could not find repository root from " + AppContext.BaseDirectory);
    }
}
