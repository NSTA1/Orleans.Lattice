using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Regression tests for issue #2088: a fully converged reconcile pass must remain
/// observable. Gating the reconcile-plan line and the symbol-embedding tally on
/// "did anything change" made a legitimately converged pass (nothing to do) and a
/// pass that never measured (a skipped arm) indistinguishable in the log, so a
/// reader polling for a convergence line was waiting for a line that by
/// construction never appeared. Both lines now log unconditionally, including
/// their zero cases.
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    [Test]
    public async Task A_converged_no_op_pass_still_logs_its_plan_and_symbol_tally()
    {
        // The exact converged scenario: one fully-processed, unchanged file, so the
        // reconcile plan is a no-op, no chunk is committed, and no symbol is embedded.
        SeedUnchanged(
            "done.cs", "class Done { }",
            symbolsProcessed: true, contentProcessed: true, tokenCount: 4, crossReferenced: true);
        var writesBefore = _harness.AtomicWrites;

        var result = await _harness.Service.RunAsync(_harness.Request(), progress: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.FilesUnchanged, Is.EqualTo(1));
            Assert.That(_harness.AtomicWrites, Is.EqualTo(writesBefore),
                "A converged pass must still commit nothing - the apply work stays gated.");

            // Before the fix these two lines were suppressed on a no-op pass, so a
            // converged repository logged nothing at all about the pass that
            // confirmed it was converged.
            Assert.That(
                _harness.LogEntries.Any(e =>
                    e.Level == LogLevel.Information
                    && e.Message.Contains("plan -", StringComparison.Ordinal)
                    && e.Message.Contains("0 added", StringComparison.Ordinal)
                    && e.Message.Contains("1 unchanged", StringComparison.Ordinal)),
                Is.True,
                "A converged no-op pass must still log its reconcile plan.");
            Assert.That(
                _harness.LogEntries.Any(e =>
                    e.Level == LogLevel.Information
                    && e.Message.Contains("embedded 0 symbol passage(s)", StringComparison.Ordinal)),
                Is.True,
                "A converged pass that embeds no symbols must still log the zero tally.");
        });
    }
}
