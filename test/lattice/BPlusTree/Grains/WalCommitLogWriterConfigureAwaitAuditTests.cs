using System.IO;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Audit gate: <c>WalCommitLogWriter</c> is a singleton helper invoked from
/// grain context (the leaf grain's foreground commit path, the shard-root
/// saga terminal path, ...). Internal awaits in this file MUST NOT carry
/// <c>.ConfigureAwait(false)</c>, because silently dropping the grain
/// context on a helper reachable from a grain turn is fragile - it makes
/// the resume-context of every internal await unclear to readers and is
/// one bug-fix away from breaking the single-threaded-turn invariant for
/// any state added to the helper later.
/// <para>
/// The only exception is the deliberate wedge-attribution dispatch path:
/// the four outbound shard-RPC awaits whose catch must land off a
/// possibly-wedged grain context so the writer-side diagnostic counter /
/// log line still fires. Each retained call site is annotated inline with
/// the rationale.
/// </para>
/// <para>
/// This test fixes the count at exactly four so a regression that
/// reintroduces <c>.ConfigureAwait(false)</c> on a non-dispatch await
/// trips immediately. If the writer legitimately grows or loses an
/// outbound shard-RPC seam, update the expected count alongside the
/// surrounding inline comment.
/// </para>
/// </summary>
[TestFixture]
public class WalCommitLogWriterConfigureAwaitAuditTests
{
    [Test]
    public void WalCommitLogWriter_only_uses_ConfigureAwait_on_the_four_deliberate_dispatch_sites()
    {
        var path = LocateWriterSource();
        var lines = File.ReadAllLines(path);

        var hits = new List<(int LineNumber, string Text)>();
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            // Skip XML-doc and inline rationale comments; we only care
            // about real call-site code uses.
            var trimmed = line.TrimStart();
            if (trimmed.StartsWith("//", StringComparison.Ordinal)
                || trimmed.StartsWith("///", StringComparison.Ordinal))
            {
                continue;
            }
            if (line.Contains(".ConfigureAwait(false)", StringComparison.Ordinal))
            {
                hits.Add((i + 1, line.Trim()));
            }
        }

        Assert.That(
            hits.Count,
            Is.EqualTo(4),
            "WalCommitLogWriter must use .ConfigureAwait(false) ONLY on the four deliberate wedge-attribution outbound shard-RPC dispatch sites "
            + "(AppendAsync infinite + bounded, AppendBatchAsync infinite + bounded). Every other internal await must run on the caller's grain context. "
            + "Current uses:" + Environment.NewLine
            + string.Join(Environment.NewLine, hits.Select(h => $"  line {h.LineNumber}: {h.Text}")));

        // Each retained site must call into the shard grain - never a
        // local helper - so the exception scope stays the documented one.
        foreach (var (lineNumber, text) in hits)
        {
            Assert.That(
                text.Contains("grain.AppendAsync(", StringComparison.Ordinal)
                || text.Contains("grain.AppendBatchAsync(", StringComparison.Ordinal)
                || text.Contains("grainCall.WaitAsync(", StringComparison.Ordinal),
                Is.True,
                $"Retained .ConfigureAwait(false) at line {lineNumber} is not on a recognised wedge-attribution outbound shard-RPC dispatch site: {text}");
        }
    }

    private static string LocateWriterSource()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "src", "lattice", "BPlusTree", "Grains", "WalCommitLogWriter.cs");
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = dir.Parent;
        }
        throw new InvalidOperationException(
            "Could not locate src/lattice/BPlusTree/Grains/WalCommitLogWriter.cs from " + AppContext.BaseDirectory);
    }
}
