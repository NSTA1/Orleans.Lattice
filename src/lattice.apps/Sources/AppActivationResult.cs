using System.Reflection;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The structured outcome of <see cref="IAppActivationHandle.ActivateAsync"/>. A failure is data rather
/// than an exception, so it fails that app's activation alone and never silo startup.
/// </summary>
public sealed class AppActivationResult
{
    private AppActivationResult(Assembly? assembly, IReadOnlyList<AppManifestError> errors)
    {
        Assembly = assembly;
        Errors = errors;
    }

    /// <summary>Whether the app's code is available.</summary>
    public bool IsActivated => Assembly is not null;

    /// <summary>The assembly carrying the app's code when activated, otherwise null.</summary>
    public Assembly? Assembly { get; }

    /// <summary>Read-only diagnostics; empty when activated and non-empty otherwise.</summary>
    public IReadOnlyList<AppManifestError> Errors { get; }

    /// <summary>Creates a successful outcome.</summary>
    /// <param name="assembly">The assembly carrying the app's code.</param>
    public static AppActivationResult Activated(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        return new(assembly, []);
    }

    /// <summary>Creates a failed outcome.</summary>
    /// <param name="errors">The non-empty diagnostics explaining the failure; copied.</param>
    public static AppActivationResult Failed(IReadOnlyList<AppManifestError> errors)
    {
        ArgumentNullException.ThrowIfNull(errors);
        if (errors.Count == 0)
            throw new ArgumentException("A failed activation requires at least one error.", nameof(errors));
        var copy = new AppManifestError[errors.Count];
        for (var i = 0; i < copy.Length; i++)
            copy[i] = errors[i] ?? throw new ArgumentException("Errors cannot contain null.", nameof(errors));
        return new(null, copy);
    }
}
