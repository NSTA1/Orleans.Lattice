using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Turns the apps engine's value-shaped failures (registry transition errors,
/// activation outcomes, source statuses) into the exception categories the
/// app-control contract promises: <see cref="KeyNotFoundException"/> for an
/// absent app or version, and <see cref="InvalidOperationException"/> for every
/// failed precondition or activation. Messages are facade-authored; any engine
/// diagnostic appended to them is sanitized of composed tree ids first.
/// </summary>
internal static class AppsControlFailures
{
    /// <summary>The exception for an app that has no live installation.</summary>
    /// <param name="slug">The app slug.</param>
    /// <returns>The exception to throw.</returns>
    public static KeyNotFoundException NotInstalled(AppSlug slug) =>
        new($"App '{slug}' is not installed.");

    /// <summary>The exception for an app version the source cannot supply.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="version">The requested version.</param>
    /// <returns>The exception to throw.</returns>
    public static KeyNotFoundException SourceNotFound(AppSlug slug, AppVersion version) =>
        new($"App '{slug}' version '{version}' is not available from the app source.");

    /// <summary>The exception for a source result that resolved to neither a manifest nor an absence.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="result">The unresolved source result.</param>
    /// <returns>The exception to throw.</returns>
    public static InvalidOperationException SourceUnusable(AppSlug slug, AppSourceResult result) =>
        new($"App '{slug}' could not be read from the app source ({result.Status})"
            + Detail(result.Errors, slug) + ".");

    /// <summary>The exception for a rejected registry transition.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="verb">The verb that was attempted, for the message.</param>
    /// <param name="result">The rejected transition.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception FromTransition(AppSlug slug, string verb, AppRegistryTransitionResult result) =>
        result.Error switch
        {
            AppRegistryTransitionError.NotInstalled => NotInstalled(slug),
            AppRegistryTransitionError.AlreadyInstalled => new InvalidOperationException(
                $"App '{slug}' is already installed at that version; update its consent instead, or uninstall it first."),
            _ => new InvalidOperationException(
                $"Could not {verb} app '{slug}' ({result.Error})"
                + Suffix(AppsControlExceptionSanitizer.SanitizeText(result.Message, slug.Value)) + "."),
        };

    /// <summary>The exception for a failed activation-pipeline run.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="outcome">The failed outcome.</param>
    /// <param name="preface">Optional text stating what had already been recorded.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception FromActivation(AppSlug slug, AppActivationOutcome outcome, string? preface = null)
    {
        if (outcome.Failure == AppActivationFailure.NotInstalled && preface is null)
        {
            return NotInstalled(slug);
        }

        var operation = outcome.Operation.ToString().ToLowerInvariant();
        return new InvalidOperationException(
            preface
            + $"The {operation} of app '{slug}' failed ({outcome.Failure})"
            + Detail(outcome.Diagnostics, slug) + ".");
    }

    private static string Detail(IReadOnlyList<AppManifestError> errors, AppSlug slug)
    {
        if (errors is null || errors.Count == 0 || errors[0] is not { } first)
        {
            return string.Empty;
        }

        return Suffix(AppsControlExceptionSanitizer.SanitizeText(first.Message, slug.Value));
    }

    private static string Suffix(string? detail) =>
        string.IsNullOrEmpty(detail) ? string.Empty : ": " + detail.TrimEnd('.');
}
