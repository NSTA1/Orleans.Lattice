using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="TerminalDecisionGuard"/> - the
/// shared write-once terminal-recording guard the production registry grain
/// (<c>TxRegistryGrain.MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> and the
/// mixed-outcome guard in <c>RecordTerminalArrivalAsync</c>) executes. These pin
/// the exact classify truth table and, because the registry is turn-serialized,
/// exhaustively cover every terminal-delivery ordering over the {commit, abort}
/// alphabet so the "never both commit and abort" invariant is nailed here.
/// </summary>
[TestFixture]
public sealed class TerminalDecisionGuardTests
{
    [Test]
    public void Absent_admits_any_terminal([Values] bool incomingCommitted)
    {
        Assert.That(
            TerminalDecisionGuard.Classify(hasExisting: false, TxStatus.InFlight, incomingCommitted),
            Is.EqualTo(TerminalRecordAction.Record));
    }

    [Test]
    public void Recorded_inflight_admits_any_terminal([Values] bool incomingCommitted)
    {
        // A present-but-InFlight entry is not a terminal, so the incoming terminal
        // is the first authoritative outcome and must be recorded.
        Assert.That(
            TerminalDecisionGuard.Classify(hasExisting: true, TxStatus.InFlight, incomingCommitted),
            Is.EqualTo(TerminalRecordAction.Record));
    }

    [Test]
    public void Same_terminal_is_idempotent([Values] bool incomingCommitted)
    {
        var existing = incomingCommitted ? TxStatus.Committed : TxStatus.Aborted;
        Assert.That(
            TerminalDecisionGuard.Classify(hasExisting: true, existing, incomingCommitted),
            Is.EqualTo(TerminalRecordAction.Idempotent));
    }

    [Test]
    public void Opposite_terminal_conflicts([Values] bool incomingCommitted)
    {
        var existing = incomingCommitted ? TxStatus.Aborted : TxStatus.Committed;
        Assert.That(
            TerminalDecisionGuard.Classify(hasExisting: true, existing, incomingCommitted),
            Is.EqualTo(TerminalRecordAction.Conflict));
    }

    [Test]
    public void Commit_after_commit_is_idempotent() =>
        Assert.That(
            TerminalDecisionGuard.Classify(hasExisting: true, TxStatus.Committed, incomingCommitted: true),
            Is.EqualTo(TerminalRecordAction.Idempotent));

    [Test]
    public void Abort_after_abort_is_idempotent() =>
        Assert.That(
            TerminalDecisionGuard.Classify(hasExisting: true, TxStatus.Aborted, incomingCommitted: false),
            Is.EqualTo(TerminalRecordAction.Idempotent));

    /// <summary>
    /// The write-once safety property, checked by replaying every ordering of a
    /// pair of terminals for one saga through the guard against a single-slot
    /// recorded-status model. Whatever the order, the guard never lets the first
    /// recorded terminal flip: a same-terminal replay is idempotent and an
    /// opposite terminal is rejected as a conflict, so the model's recorded value
    /// is write-once.
    /// </summary>
    [Test]
    public void Never_flips_recorded_terminal_under_any_ordering(
        [Values] bool firstCommitted,
        [Values] bool secondCommitted)
    {
        bool hasRecorded = false;
        TxStatus recorded = TxStatus.InFlight;

        Apply(firstCommitted, ref hasRecorded, ref recorded);
        var afterFirst = recorded;

        Apply(secondCommitted, ref hasRecorded, ref recorded);

        // Whatever the pair and order, the first recorded terminal is write-once:
        // a same-terminal replay is idempotent and an opposing terminal is rejected
        // as a conflict, so the recorded value never changes after it is set.
        Assert.That(recorded, Is.EqualTo(afterFirst));

        static void Apply(bool committed, ref bool hasRecorded, ref TxStatus recorded)
        {
            switch (TerminalDecisionGuard.Classify(hasRecorded, recorded, committed))
            {
                case TerminalRecordAction.Record:
                    recorded = committed ? TxStatus.Committed : TxStatus.Aborted;
                    hasRecorded = true;
                    break;
                case TerminalRecordAction.Idempotent:
                    break;
                case TerminalRecordAction.Conflict:
                    // The registry throws here; the recorded value is left intact.
                    break;
            }
        }
    }
}
