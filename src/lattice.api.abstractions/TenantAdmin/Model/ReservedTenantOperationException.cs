namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a tenant-administration operation targets the reserved
/// legacy-adoption default tenant in a way the lifecycle forbids (suspend or
/// delete). The default tenant is the well-known tenant a cluster with no tenancy
/// add-on resolves to, so suspending or deleting it would fence or destroy the
/// cluster's own legacy state; the control facade rejects it fail-closed. A
/// transport binding surfaces this as an invalid-argument / failed-precondition
/// outcome. Mirrors the sibling plain-exception shape, carrying the offending
/// tenant id and the rejected operation.
/// </summary>
public sealed class ReservedTenantOperationException : Exception
{
    /// <summary>Initialises the exception for <paramref name="tenantId"/> and <paramref name="operation"/>.</summary>
    /// <param name="tenantId">The reserved tenant the operation was rejected for.</param>
    /// <param name="operation">The rejected operation (for example <c>suspend</c> or <c>delete</c>).</param>
    public ReservedTenantOperationException(string tenantId, string operation)
        : base($"Operation '{operation}' is not permitted on the reserved default tenant '{tenantId}'.")
    {
        TenantId = tenantId;
        Operation = operation;
    }

    /// <summary>The reserved tenant the operation was rejected for.</summary>
    public string TenantId { get; }

    /// <summary>The rejected operation.</summary>
    public string Operation { get; }
}
