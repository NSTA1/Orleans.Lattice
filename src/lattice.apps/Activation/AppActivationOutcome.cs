namespace Orleans.Lattice.Apps;

/// <summary>
/// The structured result of one <see cref="IAppActivationPipeline"/> run. A failed run
/// carries its <see cref="Failure"/> reason and the <see cref="Diagnostics"/> that explain
/// it; the outcome is also recorded against the app so it can be reported later through
/// <see cref="IAppActivationPipeline.GetStatusAsync"/>.
/// </summary>
[GenerateSerializer, Alias(AppActivationTypeAliases.AppActivationOutcome)]
public sealed record AppActivationOutcome
{
    /// <summary>The tenant the app is installed for.</summary>
    [Id(0)] public required TenantId Tenant { get; init; }

    /// <summary>The app the run concerned.</summary>
    [Id(1)] public required AppSlug Slug { get; init; }

    /// <summary>The operation the run performed.</summary>
    [Id(2)] public required AppActivationOperation Operation { get; init; }

    /// <summary>Why the run failed, or <see cref="AppActivationFailure.None"/> when it succeeded.</summary>
    [Id(3)] public AppActivationFailure Failure { get; init; }

    /// <summary>The installed version the run acted on, when a registry record was found.</summary>
    [Id(4)] public AppVersion? Version { get; init; }

    /// <summary>The app's registry state after the run, when a registry record was found.</summary>
    [Id(5)] public AppRegistryLifecycleState? State { get; init; }

    /// <summary>Whether the run changed the registry state.</summary>
    [Id(6)] public bool Changed { get; init; }

    /// <summary>The diagnostics explaining a failure; empty when the run succeeded.</summary>
    [Id(7)] public IReadOnlyList<AppManifestError> Diagnostics { get; init; } = Array.Empty<AppManifestError>();

    /// <summary>When the run completed.</summary>
    [Id(8)] public DateTimeOffset CompletedAtUtc { get; init; }

    /// <summary>Whether the run succeeded.</summary>
    public bool Succeeded => Failure == AppActivationFailure.None;
}
