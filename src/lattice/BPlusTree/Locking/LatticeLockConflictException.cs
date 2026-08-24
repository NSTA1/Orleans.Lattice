namespace Orleans.Lattice;

/// <summary>
/// Thrown by <see cref="ILatticeLockGrain.RenewAsync"/> when the presented
/// <see cref="LockToken"/> is not the lock's current holder token - the lease was
/// already superseded (it expired and was reclaimed, then re-granted to another
/// waiter) or the token never held the lock. The caller must treat its lease as
/// lost: stop guarding the protected resource and re-acquire if it still needs
/// the lock.
/// <para>
/// <see cref="ILatticeLockGrain.ReleaseAsync"/> does <b>not</b> throw this; a
/// release with a stale token is a silent no-op (releasing a lease you no longer
/// hold is harmless), whereas a renew must fail loudly so a paused holder learns
/// it was fenced out rather than believing it extended a lock it has lost.
/// </para>
/// <para>
/// Derives directly from <see cref="System.Exception"/> so the
/// <c>[GenerateSerializer]</c> exception needs no companion deep-copier: Orleans'
/// same-silo deep-copy path finds a base-type copier for
/// <see cref="System.Exception"/> directly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeLockConflict)]
public sealed class LatticeLockConflictException : Exception
{
    /// <summary>
    /// The name of the lock whose renew was rejected. Empty on the parameterless
    /// constructor; populated on the production overloads so caller-side
    /// diagnostics can attribute the fencing rejection without parsing the
    /// message.
    /// </summary>
    [Id(0)]
    public string LockName { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="LockName"/>. Provided to satisfy the framework's exception
    /// construction contract; production throw sites use the overloads that carry
    /// diagnostic context.
    /// </summary>
    public LatticeLockConflictException()
    {
        LockName = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and an
    /// empty <see cref="LockName"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which renew was rejected and why.</param>
    public LatticeLockConflictException(string message) : base(message)
    {
        LockName = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and an empty <see cref="LockName"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which renew was rejected and why.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeLockConflictException(string message, Exception innerException)
        : base(message, innerException)
    {
        LockName = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and the
    /// name of the lock whose renew was rejected. The primary production throw
    /// shape.
    /// </summary>
    /// <param name="message">Diagnostic context describing which renew was rejected and why.</param>
    /// <param name="lockName">The name of the lock whose renew was rejected.</param>
    public LatticeLockConflictException(string message, string lockName) : base(message)
    {
        ArgumentNullException.ThrowIfNull(lockName);
        LockName = lockName;
    }
}
