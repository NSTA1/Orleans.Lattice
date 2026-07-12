namespace Orleans.Lattice.Schema;

/// <summary>
/// The result of a schema-remediation dry-run: whether every existing value, once
/// rewritten by the caller-supplied remediation transform, satisfies a candidate
/// policy, or - on the first failure - the offending key, the reason, and a bounded
/// preview of the offending (post-transform, or original-on-transform-failure)
/// value. A dry-run is the read-only precursor to a background shadow build: it
/// proves a policy change is remediable before any physical tree is built or cut
/// over.
/// </summary>
/// <remarks>
/// This is a plain in-process result (not an Orleans-serialized type). Persisting
/// the remediation transform as durable coordinator state and performing the
/// physical shadow-build cutover are the deferred follow-up; see
/// <see cref="LatticeSchemaRemediation"/>.
/// </remarks>
public readonly record struct LatticeSchemaRemediationOutcome
{
    private LatticeSchemaRemediationOutcome(
        bool succeeded,
        int scannedCount,
        string? offendingKey,
        string? reason,
        byte[]? offendingValuePreview)
    {
        Succeeded = succeeded;
        ScannedCount = scannedCount;
        OffendingKey = offendingKey;
        Reason = reason;
        OffendingValuePreview = offendingValuePreview;
    }

    /// <summary>Whether every scanned value remediates to a policy-valid value.</summary>
    public bool Succeeded { get; }

    /// <summary>
    /// The number of entries scanned. On success this is the whole tree; on abort
    /// it is the count inspected up to and including the offending entry.
    /// </summary>
    public int ScannedCount { get; }

    /// <summary>The first offending key, or <c>null</c> on success.</summary>
    public string? OffendingKey { get; }

    /// <summary>
    /// The reason the offending value failed (a policy-violation reason, or a
    /// transform-failure message), or <c>null</c> on success.
    /// </summary>
    public string? Reason { get; }

    /// <summary>
    /// A bounded preview of the offending value's bytes (post-transform when the
    /// transform succeeded, otherwise the original), or <c>null</c> on success.
    /// </summary>
    public byte[]? OffendingValuePreview { get; }

    /// <summary>Creates a successful outcome over <paramref name="scannedCount"/> entries.</summary>
    /// <param name="scannedCount">The number of entries scanned.</param>
    public static LatticeSchemaRemediationOutcome Success(int scannedCount) =>
        new(true, scannedCount, null, null, null);

    /// <summary>Creates an aborted outcome describing the first offending entry.</summary>
    /// <param name="scannedCount">The number of entries scanned up to and including the offender.</param>
    /// <param name="offendingKey">The offending key.</param>
    /// <param name="reason">Why the entry could not be remediated.</param>
    /// <param name="offendingValuePreview">A bounded preview of the offending value.</param>
    public static LatticeSchemaRemediationOutcome Aborted(
        int scannedCount, string offendingKey, string reason, byte[] offendingValuePreview) =>
        new(false, scannedCount, offendingKey, reason, offendingValuePreview);
}
