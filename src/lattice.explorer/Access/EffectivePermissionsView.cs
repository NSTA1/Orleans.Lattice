using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The result of an EffectivePermissions query: the outcome <see cref="Status"/>,
/// an optional <see cref="Message"/> (populated on a denial or failure), and the
/// <see cref="Permissions"/> when the query succeeded. Computed entirely by the
/// facade from the live policy store, so the UI renders it verbatim and never
/// re-implements decision logic.
/// </summary>
public sealed record EffectivePermissionsView
{
    /// <summary>The outcome category of the query.</summary>
    public required AccessOperationStatus Status { get; init; }

    /// <summary>A human-readable message, populated on a denial or failure.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>The facade's effective permissions, or <see langword="null"/> on a denial or failure.</summary>
    public AuthEffectivePermissions? Permissions { get; init; }

    /// <summary><see langword="true"/> when the query succeeded.</summary>
    public bool IsSuccess => Status == AccessOperationStatus.Succeeded;
}
