namespace Orleans.Lattice;

/// <summary>
/// A single entry read from an <see cref="ILatticeQueue{T}"/>: the monotonic
/// id the queue assigned at enqueue time together with the deserialized
/// value. The id is stable for the lifetime of the parked entry, so callers
/// can correlate a peek read with later state.
/// </summary>
/// <typeparam name="T">The queued value type.</typeparam>
/// <param name="EntryId">The monotonic per-queue id assigned at enqueue time.</param>
/// <param name="Value">The deserialized queued value.</param>
public readonly record struct LatticeQueueEntry<T>(long EntryId, T Value);
