using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a delegated tenant-access write would reach outside the tenant:
/// a tenant group nested into a group the tenant does not own, an entry naming
/// another tenant's group, or a tenant-tier rule on a tree or operation the tenant
/// may not govern. <see cref="Rule"/> names the violated confinement rule. The
/// refusal fails closed: nothing is written before it is raised.
/// </summary>
/// <remarks>
/// <para>
/// The confinement rules hold for every caller, platform operators included, so
/// this is a property of the request rather than of the caller's authority, and
/// never an authorization failure.
/// </para>
/// <para>
/// Derives from <see cref="ArgumentException"/> because the refusal is caused by a
/// caller-supplied argument, so a transport binding maps it to an invalid-argument
/// status and existing <see cref="ArgumentException"/> handlers keep working. It is
/// raised in-process by the facades and is not an Orleans serializable type; the
/// no-op copier beside it keeps it intact should it ever cross a same-silo grain
/// boundary.
/// </para>
/// </remarks>
public sealed class TenantAccessConfinementException : ArgumentException
{
    /// <summary>
    /// Initialises the exception for a write to <paramref name="tenantId"/> that
    /// violated <paramref name="rule"/>.
    /// </summary>
    /// <param name="tenantId">The tenant id the refused write named.</param>
    /// <param name="rule">The confinement rule the write violated.</param>
    /// <param name="message">A self-contained, caller-facing description of the violation. Must not be <c>null</c>.</param>
    /// <param name="paramName">The name of the offending argument, or <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <c>null</c>.</exception>
    public TenantAccessConfinementException(
        string tenantId, TenantAccessConfinementRule rule, string message, string? paramName = null)
        : base(message ?? throw new ArgumentNullException(nameof(message)), paramName)
    {
        TenantId = tenantId;
        Rule = rule;
    }

    /// <summary>The tenant id the refused write named.</summary>
    public string TenantId { get; }

    /// <summary>The confinement rule the write violated.</summary>
    public TenantAccessConfinementRule Rule { get; }
}

/// <summary>
/// No-op deep copier for <see cref="TenantAccessConfinementException"/>. A
/// generated copier for an exception deriving from a BCL exception subclass asks
/// for a base-type copier Orleans does not register, so a same-silo copy would
/// fail with an opaque <c>KeyNotFoundException</c>. An exception is immutable once
/// constructed, so returning the same instance is a correct deep copy.
/// </summary>
[RegisterCopier]
internal sealed class TenantAccessConfinementExceptionCopier : IDeepCopier<TenantAccessConfinementException>
{
    /// <inheritdoc />
    public TenantAccessConfinementException DeepCopy(TenantAccessConfinementException input, CopyContext context) => input;
}
