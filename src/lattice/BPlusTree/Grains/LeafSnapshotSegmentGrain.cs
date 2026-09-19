using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafSnapshotSegmentGrain"/> implementation. Holds a
/// single persisted <see cref="LeafSnapshotSegment"/> via the lattice storage
/// provider configured by <see cref="LatticeOptions.StorageProviderName"/>.
/// <para>
/// One read, one write, one clear, and no snapshot semantics whatsoever. The
/// separate state name keeps a segment row distinct from the manifest row that
/// <see cref="LeafSnapshotStorageGrain"/> owns, so the two are read
/// independently and a segment read never drags the manifest in with it.
/// </para>
/// </summary>
internal sealed class LeafSnapshotSegmentGrain(
    IGrainContext context,
    [PersistentState("leaf-snapshot-segment", LatticeOptions.StorageProviderName)]
    IPersistentState<LeafSnapshotSegment> state) : ILeafSnapshotSegmentGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task SaveAsync(byte[] frame, int rowCount, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frame);
        cancellationToken.ThrowIfCancellationRequested();

        // Refuse a frame that does not validate rather than persisting it.
        // A segment is written BEFORE the manifest that commits it, so a bad
        // frame accepted here would be referenced by a manifest that then
        // reports coverage the snapshot cannot reproduce. Throwing aborts the
        // capture before the manifest is written, which leaves the previous
        // snapshot - whatever it was - intact and authoritative.
        if (frame.Length == 0 || !LeafSnapshotCodec.Validate(frame))
        {
            throw new ArgumentException(
                "Segment frame is empty or does not decode as a leaf-snapshot frame.",
                nameof(frame));
        }

        state.State.Frame = frame;
        state.State.RowCount = rowCount;
        await state.WriteStateAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task<byte[]?> LoadFrameAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var frame = state.State.Frame;
        if (frame is not { Length: > 0 })
        {
            return Task.FromResult<byte[]?>(null);
        }

        // Fail closed on a frame that no longer decodes, and on one whose row
        // count disagrees with what the writer recorded. Both are reported as
        // absence, which the caller turns into "this segmented snapshot is not
        // usable" and falls back to WAL replay. Returning the frame anyway
        // would hand the caller a snapshot with silently fewer rows, and the
        // coverage-gated WAL GC would then have trimmed a prefix nothing can
        // reproduce. This mirrors LeafSnapshotBlob.ValidateRowPayload's
        // fail-closed contract on the unsegmented path.
        if (!LeafSnapshotCodec.Validate(frame)
            || !LeafSnapshotCodec.TryGetRowCount(frame, out var decodedRows)
            || decodedRows != state.State.RowCount)
        {
            return Task.FromResult<byte[]?>(null);
        }

        // Returned by reference; Frame is [Immutable] so a same-silo call
        // shares the array rather than allocating a second window-sized copy.
        return Task.FromResult<byte[]?>(frame);
    }

    /// <inheritdoc />
    public async Task ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (state.State.Frame is null)
        {
            // Nothing to clear; ClearStateAsync still touches the provider,
            // so short-circuit to keep idempotent calls I/O-free.
            return;
        }

        await state.ClearStateAsync().ConfigureAwait(true);
        state.State = new LeafSnapshotSegment();
    }
}
