using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.TraceValidation;

/// <summary>
/// Records the atomic-commit protocol events production emits through
/// <see cref="AtomicCommitTrace.SinkForTest"/> and maps one saga's events onto
/// the vocabulary of <c>spec/atomic-commit/AtomicCommit.tla</c>.
/// <para>
/// The emitting grains run concurrently on the test cluster's silos, so events
/// are appended to a lock-free queue. Each emission happens synchronously after
/// the step it reports and before the emitting turn replies to anyone, so an
/// event that causally follows another is always appended after it; events with
/// no causal order (two leaves applying the same terminal) land in either order,
/// and the specification admits both.
/// </para>
/// </summary>
internal sealed class AtomicCommitTraceRecorder : IAtomicCommitTraceSink
{
    private readonly ConcurrentQueue<TraceEvent> _events = new();

    /// <inheritdoc />
    public void OnPrepareResolved(string treeId, Guid transactionId, IReadOnlyList<KeyValuePair<string, byte[]>> keys, bool everyParticipantPrepared)
    {
        var copy = new string[keys.Count];
        for (var i = 0; i < copy.Length; i++)
        {
            copy[i] = keys[i].Key;
        }

        _events.Enqueue(new TraceEvent(TraceEventKind.Prepare, transactionId, treeId, copy, everyParticipantPrepared));
    }

    /// <inheritdoc />
    public void OnDecisionRecorded(Guid transactionId, bool committed) =>
        _events.Enqueue(new TraceEvent(TraceEventKind.Decide, transactionId, null, [], committed));

    /// <inheritdoc />
    public void OnLeafTerminalApplied(Guid transactionId, IEnumerable<string> keys, bool committed, bool orphan) =>
        _events.Enqueue(new TraceEvent(
            orphan ? TraceEventKind.OrphanDrain : TraceEventKind.Terminal,
            transactionId,
            null,
            keys.Order(StringComparer.Ordinal).ToArray(),
            committed));

    /// <inheritdoc />
    public void OnDecisionForgotten(Guid transactionId) =>
        _events.Enqueue(new TraceEvent(TraceEventKind.Forget, transactionId, null, [], false));

    /// <summary>
    /// The transaction ids of every saga on <paramref name="treeId"/> whose
    /// prepare outcome was recorded, in recording order.
    /// </summary>
    public IReadOnlyList<Guid> SagasOn(string treeId) =>
        _events
            .Where(e => e.Kind == TraceEventKind.Prepare && e.TreeId == treeId)
            .Select(e => e.TransactionId)
            .Distinct()
            .ToList();

    /// <summary>
    /// One line per raw recorded event of <paramref name="transactionId"/>, in
    /// recording order, for diagnostics: which grain-level step produced which
    /// spec steps.
    /// </summary>
    public IEnumerable<string> DescribeSaga(Guid transactionId) =>
        _events
            .Where(e => e.TransactionId == transactionId)
            .Select(e => $"{e.Kind}({string.Join(",", e.Keys)}{(e.Keys.Length > 0 ? ";" : string.Empty)}{e.Flag})");

    /// <summary>
    /// Maps the recorded events of one saga onto spec-level trace events. The
    /// saga becomes model value <c>t1</c> and its keys, in ordinal order, become
    /// <c>k1</c>, <c>k2</c>, ... - <c>AtomicCommit.tla</c> fixes <c>t1</c>'s
    /// participants as <c>{k1, k2}</c>, so a saga of any other width is reported
    /// as a mismatch by the specification rather than by this mapping. A leaf
    /// terminal covering several keys becomes one spec step per key, because the
    /// specification broadcasts one key at a time.
    /// </summary>
    public IReadOnlyList<SpecTraceEvent> MapSaga(Guid transactionId)
    {
        var events = _events.Where(e => e.TransactionId == transactionId).ToList();
        var prepare = events.FirstOrDefault(e => e.Kind == TraceEventKind.Prepare)
            ?? throw new InvalidOperationException($"no prepare outcome was recorded for saga {transactionId:N}.");

        var keyNames = prepare.Keys
            .Order(StringComparer.Ordinal)
            .Select((key, index) => (key, name: "k" + (index + 1).ToString(CultureInfo.InvariantCulture)))
            .ToDictionary(p => p.key, p => p.name, StringComparer.Ordinal);
        var names = keyNames.Values.Order(StringComparer.Ordinal).ToArray();

        var mapped = new List<SpecTraceEvent>();
        foreach (var e in events)
        {
            switch (e.Kind)
            {
                case TraceEventKind.Prepare:
                    // The coordinator reports only the aggregate prepare outcome, which
                    // is all its SagaCoordinatorCore feed has either: on a pivot to
                    // compensation one participant stands in as the witness nack.
                    mapped.Add(SpecTraceEvent.Prepare("t1", names, e.Flag ? names : names[1..]));
                    break;
                case TraceEventKind.Decide:
                    mapped.Add(SpecTraceEvent.Decide("t1", e.Flag ? "committed" : "aborted"));
                    break;
                case TraceEventKind.Terminal:
                    foreach (var key in e.Keys)
                    {
                        mapped.Add(SpecTraceEvent.Terminal("t1", keyNames[key], e.Flag ? "commit" : "abort"));
                    }
                    break;
                case TraceEventKind.OrphanDrain:
                    foreach (var key in e.Keys)
                    {
                        mapped.Add(SpecTraceEvent.OrphanDrain("t1", keyNames[key]));
                    }
                    break;
                case TraceEventKind.Forget:
                    mapped.Add(SpecTraceEvent.Forget("t1"));
                    break;
            }
        }

        return mapped;
    }

    /// <summary>
    /// Renders spec-level trace events as the <c>AtomicCommitTraceData</c>
    /// module the trace specification extends.
    /// </summary>
    public static string RenderTraceModule(IReadOnlyList<SpecTraceEvent> trace)
    {
        var builder = new StringBuilder();
        builder.Append("---- MODULE AtomicCommitTraceData ----\n");
        builder.Append("\\* Generated by AtomicCommitTraceRecorder from a recorded trace.\n");
        builder.Append("TraceLog == <<\n");
        for (var i = 0; i < trace.Count; i++)
        {
            builder.Append("  ").Append(trace[i].ToTla());
            builder.Append(i + 1 < trace.Count ? ",\n" : "\n");
        }
        builder.Append(">>\n====\n");
        return builder.ToString();
    }

    private enum TraceEventKind
    {
        Prepare,
        Decide,
        Terminal,
        OrphanDrain,
        Forget,
    }

    private sealed record TraceEvent(TraceEventKind Kind, Guid TransactionId, string? TreeId, string[] Keys, bool Flag);
}
