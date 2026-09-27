namespace Orleans.Lattice.Apps;

/// <summary>
/// The structured outcome of an app-registry lifecycle transition. A rejected
/// transition is reported here rather than thrown, so a facade can map it onto its own
/// status vocabulary; only an authorization denial or a programmer error throws.
/// </summary>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppRegistryTransitionResult)]
public sealed record AppRegistryTransitionResult
{
    /// <summary>
    /// The record after the call: the newly written record on an applied transition, the
    /// unchanged record on an idempotent no-op or a rejection, or <c>null</c> when no
    /// record exists.
    /// </summary>
    [Id(0)] public AppRegistryRecord? Record { get; init; }

    /// <summary>Why the transition was rejected, or <see cref="AppRegistryTransitionError.None"/>.</summary>
    [Id(1)] public AppRegistryTransitionError Error { get; init; }

    /// <summary><c>true</c> when the call wrote a new record revision; <c>false</c> for a no-op or rejection.</summary>
    [Id(2)] public bool Changed { get; init; }

    /// <summary>A human-readable diagnostic for a rejection, or <c>null</c> on success.</summary>
    [Id(3)] public string? Message { get; init; }

    /// <summary><c>true</c> when the transition was applied or was an idempotent no-op.</summary>
    public bool Succeeded => Error == AppRegistryTransitionError.None;

    /// <summary>Creates a success outcome.</summary>
    /// <param name="record">The record after the call.</param>
    /// <param name="changed">Whether a new revision was written.</param>
    /// <returns>The success outcome.</returns>
    internal static AppRegistryTransitionResult Success(AppRegistryRecord record, bool changed) =>
        new() { Record = record, Changed = changed };

    /// <summary>Creates a rejection outcome.</summary>
    /// <param name="record">The current record, or <c>null</c> when absent.</param>
    /// <param name="error">The rejection reason.</param>
    /// <param name="message">The diagnostic.</param>
    /// <returns>The rejection outcome.</returns>
    internal static AppRegistryTransitionResult Rejected(
        AppRegistryRecord? record,
        AppRegistryTransitionError error,
        string message) =>
        new() { Record = record, Error = error, Message = message };
}
