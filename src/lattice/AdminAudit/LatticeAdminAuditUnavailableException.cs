namespace Orleans.Lattice;

/// <summary>
/// Thrown by an administration facade when the durable admin audit trail is
/// enabled and the call's audit intent could not be written. The call is refused
/// <b>before</b> any change is attempted (fail closed): a privileged mutation that
/// cannot be audited does not happen.
/// </summary>
/// <remarks>
/// Derives directly from <see cref="Exception"/> so the Orleans-generated same-silo
/// deep copier finds a registered base-type copier.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAdminAuditUnavailable)]
public sealed class LatticeAdminAuditUnavailableException : Exception
{
    /// <summary>Initializes a new instance with a default message.</summary>
    public LatticeAdminAuditUnavailableException()
        : this("The admin audit trail could not record this call, so the call was refused before any change was made.")
    {
    }

    /// <summary>Initializes a new instance with the specified message.</summary>
    /// <param name="message">The error message.</param>
    public LatticeAdminAuditUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance with the specified message and inner exception.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The audit write failure.</param>
    public LatticeAdminAuditUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
