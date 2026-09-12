namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// What this container can positively say about the protection of the durable
/// agent-memory tree, as one value that a health endpoint and a metric series can
/// both report without deriving it twice.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> Issue #2640 records a deployment on which every capture
/// threw and every machine-readable surface stayed green: <c>/health/live</c> and
/// <c>/health/ready</c> carried no backup component at all, and <c>/metrics</c>
/// carried no backup series, so the only evidence was a log line. The failure was
/// not that a flag reported the wrong value; it was that no flag existed to report
/// one. This type is the single derivation both surfaces read, so they cannot drift
/// into disagreeing about the same container.
/// </para>
/// <para>
/// <b>It is a state set, not a severity scale.</b> The ordinals are stable
/// identifiers for an exposition to carry, and only
/// <see cref="Protected"/> means the tree is being captured as configured. Do not
/// read an inequality between two non-protected values as a ranking; write an alert
/// against <c>!= Protected</c> rather than against <c>&lt; n</c>.
/// </para>
/// <para>
/// <b>Never-captured is deliberately not healthy.</b> A container that has attempted
/// nothing and a container that is capturing hourly are different facts, and the
/// whole point of the reported state is that an operator can tell them apart. See
/// <see cref="NeverCaptured"/>.
/// </para>
/// </remarks>
public enum RepoContextBackupState
{
    /// <summary>
    /// No external backup sink is configured, so this container is deliberately not
    /// capturing the tree anywhere. This is a configuration statement rather than a
    /// fault: a host is required to boot without a sink, so nothing here treats the
    /// default deployment as broken. It is still reported explicitly, because
    /// "backup is off" and "backup is on and working" must not look alike.
    /// </summary>
    Disabled = 0,

    /// <summary>
    /// Backup is configured, a capture has failed, and no capture has ever
    /// succeeded in this container - so nothing this process produced is
    /// recoverable. This is the state issue #2621 sat in for 14 consecutive
    /// attempts while every surface reported healthy.
    /// </summary>
    FailingUnprotected = 1,

    /// <summary>
    /// Backup is configured and has neither succeeded nor failed yet. It is the
    /// honest startup state and it is <b>not</b> healthy: reporting it as healthy is
    /// exactly the false green that lets a container which never captures anything
    /// look identical to one that captures hourly.
    /// </summary>
    NeverCaptured = 2,

    /// <summary>
    /// Captures are completing, but the most recent full capture described zero
    /// entries, so it protects nothing. A backup job that runs over an empty or
    /// wrongly-scoped selection succeeds and reports success, which is why this is
    /// called out rather than folded into <see cref="Protected"/>.
    /// </summary>
    CapturedNothing = 3,

    /// <summary>
    /// At least one capture succeeded in this container, but the most recent attempt
    /// failed. Earlier output is still recoverable, so this is less severe than
    /// <see cref="FailingUnprotected"/>, but the configured cadence is broken and
    /// the protection is ageing.
    /// </summary>
    FailingAfterCapture = 4,

    /// <summary>
    /// Backup is configured, the most recent attempt succeeded, and the most recent
    /// full capture described at least one entry. This is the only value that means
    /// the tree is protected as configured.
    /// </summary>
    Protected = 5,
}
