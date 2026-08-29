namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// Thrown when the cluster does not serve the tenancy surface the call needs:
/// the tenant-administration binding is not hosted at all, or the specific
/// optional facade behind the RPC - cross-tenant grants, admin subjects, region
/// residency, or usage against quota - is not registered.
/// <para>
/// This is the Explorer's typed form of the gRPC <c>Unimplemented</c> status the
/// binding answers those RPCs with, so a caller can tell "this cluster has no
/// such capability" apart from "the server refused you" and from "the server
/// could not be reached". A tenancy surface degrades to nothing on this, rather
/// than rendering an error the operator can do nothing about.
/// </para>
/// </summary>
/// <remarks>
/// A plain <see cref="Exception"/> deriving directly from
/// <see cref="System.Exception"/>: it is raised and handled entirely inside the
/// Explorer process and never crosses an Orleans grain boundary, so it carries
/// no serialization attributes.
/// </remarks>
public sealed class TenancyUnavailableException : Exception
{
    /// <summary>
    /// Creates the exception with the default message.
    /// </summary>
    public TenancyUnavailableException()
        : base("This cluster does not serve tenant administration.")
    {
    }

    /// <summary>
    /// Creates the exception with <paramref name="message"/>, typically the
    /// server's own explanation of which facade is absent.
    /// </summary>
    /// <param name="message">The description of what is not served.</param>
    public TenancyUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Creates the exception with <paramref name="message"/> and the transport
    /// fault it was translated from.
    /// </summary>
    /// <param name="message">The description of what is not served.</param>
    /// <param name="innerException">The underlying transport fault.</param>
    public TenancyUnavailableException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
