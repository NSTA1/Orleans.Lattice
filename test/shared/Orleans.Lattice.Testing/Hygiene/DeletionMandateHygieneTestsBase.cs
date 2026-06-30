using System.IO;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Hygiene gate for the universal cross-cluster atomic-visibility primitive.
/// The retired apply-mode receiver-side saga surface and the retired
/// staging-buffer scaffold were superseded by the universal source+receiver
/// primitive built on per-tree <c>ITxRegistryGrain</c> + per-leaf
/// <c>_pendingTx</c> + per-shard terminal mark + receiver wire seam.
/// <para>
/// This test fails the build if any of the doomed identifiers reappear in
/// the fixture's slice. A concrete subclass supplies a
/// <see cref="HygieneScanScope"/>; only the <c>.cs</c> files under the slice
/// are scanned, so historical references in the changelog or other markdown
/// are unaffected.
/// </para>
/// </summary>
public abstract class DeletionMandateHygieneTestsBase
{
    /// <summary>
    /// Identifiers that the universal-visibility ship explicitly retired.
    /// None may appear anywhere in <c>src/</c> or <c>test/</c> going
    /// forward. Each entry is assembled at runtime from fragments so this
    /// source file itself does not contain the literal identifier and is
    /// therefore not self-flagged.
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

    /// <summary>The repository slice this fixture is responsible for scanning.</summary>
    protected abstract HygieneScanScope Scope { get; }

    /// <summary>
    /// Scans the <c>.cs</c> files in this fixture's slice and fails if any
    /// retired apply-mode / staging-buffer identifier is present. This base
    /// file is exempt (self-reference would otherwise trip the gate, even
    /// though the identifier strings here are fragment-assembled at runtime
    /// to keep the source bytes clean).
    /// </summary>
    [Test]
    public void Doomed_identifiers_have_no_remaining_references()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var violations = new List<string>();
        foreach (var file in HygieneRepository.EnumerateSliceFiles(repoRoot, Scope, "*.cs"))
        {
            var full = Path.GetFullPath(file);
            if (Path.GetFileName(full).Equals("DeletionMandateHygieneTestsBase.cs", StringComparison.OrdinalIgnoreCase)) continue;

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
}
