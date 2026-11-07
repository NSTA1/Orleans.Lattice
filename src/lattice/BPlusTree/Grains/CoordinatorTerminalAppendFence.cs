using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The append fence a saga coordinator appends its terminals through (epic #5605,
/// member 2; design note section 2, "Coordinator terminals"). While the fence gate
/// is open it stamps every append with the coordinator-terminal class
/// (<see cref="WalAppendStamp.CoordinatorTerminal"/>), which the WAL shard admits
/// without an entry check and, for a <c>TxCommit</c> or <c>TxAbort</c> record, does
/// not report in its unstamped watermark. Without it every saga would raise the
/// watermark of each partition it touched and hold every leaf writing those
/// partitions on the replay-based advance.
/// </summary>
/// <remarks>
/// <para>
/// A coordinator is not a leaf: it has no epoch, claims nothing and never arms, so
/// most of <see cref="ILeafAppendFence"/> is a no-op here. The class can never be
/// rejected (no entry is checked), so a stale or unloaded outcome is unreachable;
/// should one arrive, the fence closes its own view of the gate for
/// <see cref="LeafAppendEpochFence.MethodUnknownBackoff"/> and the append is
/// re-issued unstamped, which the WAL shard always admits.
/// </para>
/// <para>
/// A partition hosted by an older-build silo that joined before this silo's gate
/// observed it fails the stamped call with the definite "method unknown" failure;
/// the dispatch then re-issues the append unstamped and this fence closes its view of
/// the gate for the same backoff. Stamping costs no allocation: the stamp is a value
/// and is returned synchronously.
/// </para>
/// </remarks>
internal sealed class CoordinatorTerminalAppendFence : ILeafAppendFence
{
    private readonly IWalAppendFenceGate _gate;
    private long _gateClosedUntilTicks;

    /// <summary>Creates a coordinator-terminal fence over the silo's view of the fence gate.</summary>
    /// <param name="gate">The silo's view of the fence gate.</param>
    public CoordinatorTerminalAppendFence(IWalAppendFenceGate gate)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
    }

    /// <summary>
    /// The fence for a coordinator activation, or <see langword="null"/> when the
    /// silo provides no fence gate (the appends then go unstamped, as in 10.2.0).
    /// </summary>
    /// <param name="services">The activation's services; may be <see langword="null"/>.</param>
    public static CoordinatorTerminalAppendFence? Resolve(IServiceProvider? services) =>
        services?.GetService<IWalAppendFenceGate>() is { } gate ? new CoordinatorTerminalAppendFence(gate) : null;

    /// <summary>
    /// Appends one saga terminal, under the coordinator-terminal class when
    /// <paramref name="fence"/> is set and the writer supports fencing, otherwise
    /// through the existing, unstamped method.
    /// </summary>
    public static Task<long> AppendAsync(ICommitLogWriter writer, WalRecord terminal, CoordinatorTerminalAppendFence? fence, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return fence is not null && writer is IFencedCommitLogWriter fenced
            ? fenced.AppendAsync(terminal, fence, cancellationToken)
            : writer.AppendAsync(terminal, cancellationToken);
    }

    /// <summary>
    /// Appends a batch of saga terminals, as <see cref="AppendAsync"/> does for one.
    /// </summary>
    public static Task<IReadOnlyList<long>> AppendManyAsync(ICommitLogWriter writer, IReadOnlyList<WalRecord> terminals, CoordinatorTerminalAppendFence? fence, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return fence is not null && writer is IFencedCommitLogWriter fenced
            ? fenced.AppendManyAsync(terminals, fence, cancellationToken)
            : writer.AppendManyAsync(terminals, cancellationToken);
    }

    /// <summary><see langword="true"/> while this fence stamps: the gate is open and no backoff is in force.</summary>
    internal bool StampsForTesting => IsStamping();

    /// <inheritdoc />
    public ValueTask<WalAppendStamp?> StampForAsync(IWalShardGrain shard, int partition, CancellationToken cancellationToken) =>
        new(IsStamping() ? WalAppendStamp.CoordinatorTerminal : null);

    /// <inheritdoc />
    public void OnStampedAppended(int partition, long unstampedWatermark)
    {
    }

    /// <inheritdoc />
    public void OnUnstampedAppended(int partition, long highestOffset)
    {
    }

    /// <inheritdoc />
    public void OnUnloaded(int partition, WalAppendStamp stamp) => CloseGateView();

    /// <inheritdoc />
    public LeafRetiredException? OnAdmitted(long admissionMark) => null;

    /// <inheritdoc />
    public LeafRetiredException OnStale(WalAppendStamp stamp)
    {
        // Unreachable: the terminal class is never checked against an entry.
        CloseGateView();
        return new LeafRetiredException("coordinator-terminal");
    }

    /// <inheritdoc />
    public void OnStampedMethodUnknown() => CloseGateView();

    /// <inheritdoc />
    public void OnClaimsExhausted() => CloseGateView();

    /// <inheritdoc />
    /// <remarks>A coordinator terminal tracks no own appends, so there is nothing to release.</remarks>
    public void OnRefusedUnallocated()
    {
    }

    private bool IsStamping()
    {
        var until = Volatile.Read(ref _gateClosedUntilTicks);
        return (until == 0 || Environment.TickCount64 >= until) && _gate.IsOpen;
    }

    private void CloseGateView() =>
        Volatile.Write(
            ref _gateClosedUntilTicks,
            Environment.TickCount64 + (long)LeafAppendEpochFence.MethodUnknownBackoff.TotalMilliseconds);
}
