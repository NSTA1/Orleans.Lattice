namespace Orleans.Lattice.Replication;

/// <summary>
/// The in-process carrier of the source lineage an entry was stamped with
/// (issue #4707). A transport enters a scope around the apply of a batch it
/// received, and the causal-apply buffer drain and the dead-letter replay enter
/// one around each entry they apply with the stamp stored alongside it. The
/// canonical <see cref="ReplicationApplier"/> reads the scope at its admission
/// seam, so every apply entry is checked against the lineage the receiver last
/// drained through the one <see cref="ReplicationSourceLineageGate"/> call.
/// <para>
/// The scope is an <see cref="AsyncLocal{T}"/>, not a
/// <c>RequestContext</c> key: it never crosses a grain call, so a peer cannot
/// set it, and a grain hop that needs the stamp - parking an entry, or
/// dead-lettering it - passes it explicitly.
/// </para>
/// </summary>
internal sealed class ReplicationSourceLineageScope : IDisposable
{
    private static readonly AsyncLocal<ReplicationSourceLineageScope?> s_current = new();

    private readonly ReplicationSourceLineageScope? _previous;
    private Decision? _decided;
    private bool _disposed;

    private ReplicationSourceLineageScope(ReplicationSourceLineageStamp? stamp, Guid? observedFrontierEpoch)
    {
        Stamp = stamp;
        ObservedFrontierEpoch = observedFrontierEpoch;
        _previous = s_current.Value;
    }

    /// <summary>
    /// The stamp entries applied in this scope carry; <see langword="null"/> for
    /// an unstamped delivery, which applies as before.
    /// </summary>
    public ReplicationSourceLineageStamp? Stamp { get; }

    /// <summary>
    /// The receiver tree frontier epoch the transport already observed for this
    /// delivery, or <see langword="null"/> when the applier reads it itself.
    /// </summary>
    public Guid? ObservedFrontierEpoch { get; }

    /// <summary>The innermost active scope, or <see langword="null"/>.</summary>
    public static ReplicationSourceLineageScope? Active => s_current.Value;

    /// <summary>The stamp of the innermost active scope, or <see langword="null"/>.</summary>
    public static ReplicationSourceLineageStamp? Current => s_current.Value?.Stamp;

    /// <summary>
    /// Enters a scope whose applies carry <paramref name="stamp"/>. Always
    /// replaces any outer scope, so an unstamped entry applied inside a stamped
    /// delivery is not checked against the delivery's stamp.
    /// </summary>
    public static ReplicationSourceLineageScope Enter(
        ReplicationSourceLineageStamp? stamp,
        Guid? observedFrontierEpoch = null)
    {
        var scope = new ReplicationSourceLineageScope(stamp, observedFrontierEpoch);
        s_current.Value = scope;
        return scope;
    }

    /// <summary>
    /// Enters a scope for a delivery <paramref name="sourceClusterId"/> stamped
    /// with <paramref name="lineage"/>; a <see langword="null"/> lineage enters
    /// an unstamped scope.
    /// </summary>
    public static ReplicationSourceLineageScope Enter(
        string sourceClusterId,
        Guid? lineage,
        Guid? observedFrontierEpoch = null) =>
        Enter(
            lineage is { } l ? new ReplicationSourceLineageStamp(sourceClusterId, l) : null,
            observedFrontierEpoch);

    /// <summary>
    /// The verdict already reached for <paramref name="treeId"/> in this scope,
    /// so a delivery split into several runs pays for one check.
    /// </summary>
    public bool TryGetVerdict(string treeId, out ReplicationSourceLineageGate.Verdict verdict)
    {
        if (Volatile.Read(ref _decided) is { } decided
            && string.Equals(decided.TreeId, treeId, StringComparison.Ordinal))
        {
            verdict = decided.Verdict;
            return true;
        }

        verdict = default;
        return false;
    }

    /// <summary>Records the verdict reached for <paramref name="treeId"/>.</summary>
    public void Record(string treeId, ReplicationSourceLineageGate.Verdict verdict)
    {
        Volatile.Write(ref _decided, new Decision(treeId, verdict));
    }

    /// <summary>Restores the scope that was active when this one was entered.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        s_current.Value = _previous;
    }

    private sealed record Decision(string TreeId, ReplicationSourceLineageGate.Verdict Verdict);
}
