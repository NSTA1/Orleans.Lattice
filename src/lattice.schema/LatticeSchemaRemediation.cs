namespace Orleans.Lattice.Schema;

/// <summary>
/// The pure, storage-agnostic core of background schema remediation: given the
/// existing (key, value) entries of a tree, a caller-supplied remediation
/// <see cref="LatticeValueTransform"/>, and the candidate <see cref="LatticeSchemaPolicy"/>,
/// it rewrites each value and validates the result, aborting on the first entry
/// that cannot be remediated. It is the read-only dry-run that a background shadow
/// build runs first: if the dry-run aborts, no physical tree is built and the
/// original tree is untouched.
/// </summary>
/// <remarks>
/// <para>
/// <b>Scope.</b> This delivers the remediation decision logic against an entry
/// stream, fully unit-testable without a cluster. Persisting the transform as
/// durable coordinator state and performing the physical shadow build + cutover
/// (via <c>TreeRegistryEntry.PhysicalTreeId</c> aliasing) are the deferred
/// follow-up: this component is what that shadow build would call per value, and
/// its abort contract (first offending key / reason / value preview) is exactly
/// what the coordinator reports.
/// </para>
/// <para>
/// <b>Abort semantics.</b> A value whose transform throws (malformed / not
/// remediable) aborts with the transform's message and a preview of the
/// <i>original</i> value; a value that transforms cleanly but still fails the
/// candidate policy aborts with the policy reason and a preview of the
/// <i>transformed</i> value.
/// </para>
/// </remarks>
public static class LatticeSchemaRemediation
{
    /// <summary>
    /// Runs a remediation dry-run over <paramref name="entries"/>.
    /// </summary>
    /// <param name="entries">The existing (key, value) entries to remediate. Must not be <c>null</c>.</param>
    /// <param name="transform">The per-value remediation transform to apply.</param>
    /// <param name="candidatePolicy">The policy the transformed values must satisfy. Must not be <c>null</c>.</param>
    /// <param name="previewMaxBytes">Maximum leading bytes copied into an offending-value preview. Clamped to at least 1. Defaults to 4096.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>
    /// <see cref="LatticeSchemaRemediationOutcome.Success"/> when every value
    /// remediates, otherwise <see cref="LatticeSchemaRemediationOutcome.Aborted"/>
    /// describing the first offending entry.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> or <paramref name="candidatePolicy"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="candidatePolicy"/> carries an uncompilable regex rule.</exception>
    public static async Task<LatticeSchemaRemediationOutcome> DryRunAsync(
        IAsyncEnumerable<KeyValuePair<string, byte[]>> entries,
        LatticeValueTransform transform,
        LatticeSchemaPolicy candidatePolicy,
        int previewMaxBytes = 4096,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(candidatePolicy);

        var compiled = CompiledSchemaPolicy.Compile(candidatePolicy);
        var bound = Math.Max(1, previewMaxBytes);
        var scanned = 0;

        await foreach (var entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            scanned++;

            byte[] transformed;
            try
            {
                transformed = LatticeValueTransformEvaluation.Evaluate(entry.Value, in transform);
            }
            catch (InvalidOperationException ex)
            {
                return LatticeSchemaRemediationOutcome.Aborted(
                    scanned, entry.Key, ex.Message, Preview(entry.Value, bound));
            }

            var reason = compiled.Validate(transformed);
            if (reason is not null)
            {
                return LatticeSchemaRemediationOutcome.Aborted(
                    scanned, entry.Key, reason, Preview(transformed, bound));
            }
        }

        return LatticeSchemaRemediationOutcome.Success(scanned);
    }

    private static byte[] Preview(byte[]? value, int bound)
    {
        if (value is null || value.Length == 0)
        {
            return Array.Empty<byte>();
        }

        var length = Math.Min(value.Length, bound);
        return value.AsSpan(0, length).ToArray();
    }
}
