namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Pure, allocation-free decision core for the <see cref="WalCommitLogWriter"/>'s
/// shutdown-drain admission gate. Extracted from the writer so the exact
/// production refusal decision can be driven under systematic (Coyote)
/// interleaving without a silo: a violation the model finds is a violation of the
/// real drain path.
/// </summary>
/// <remarks>
/// <para>
/// On <see cref="WalCommitLogWriter.DrainAsync"/> the writer flips a drain flag
/// and cancels a per-instance drain token. Two seams cooperate to make shutdown
/// live: the <b>pre-admission gate</b> - <see cref="IsDispatchRefused"/> - refuses
/// a fresh dispatch the moment the flag is up so no new caller parks during
/// shutdown; and the <b>admission wait</b> observes the drain token so every
/// already-parked caller is released within bounded time. The wait must observe
/// the token as part of the wait itself, not check it and then park: a caller
/// that reads the token down, is preempted while the drain fires, and only then
/// parks without the token in its wait set is lost-wakeup'd and never released -
/// the silo wedges on shutdown.
/// </para>
/// </remarks>
internal static class WalAdmissionGateCore
{
    /// <summary>
    /// Decides whether a fresh WAL dispatch must be refused because the owning
    /// writer has begun draining. Refusing here keeps a shutting-down writer from
    /// registering new dispatches that would race the drain.
    /// </summary>
    /// <param name="isDraining">Whether the writer has begun draining.</param>
    /// <returns>
    /// <see langword="true"/> when the dispatch must be refused with a typed
    /// shutdown back-pressure fault; <see langword="false"/> when it may proceed
    /// to the per-partition admission wait.
    /// </returns>
    public static bool IsDispatchRefused(bool isDraining) => isDraining;
}
