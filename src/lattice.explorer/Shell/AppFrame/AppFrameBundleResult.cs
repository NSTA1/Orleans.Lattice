namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>The outcome of loading and verifying a launch's bundle.</summary>
/// <param name="Bundle">The verified bundle, or <see langword="null"/> on failure.</param>
/// <param name="Failure">Why loading failed; meaningful only when <paramref name="Bundle"/> is <see langword="null"/>.</param>
internal readonly record struct AppFrameBundleResult(AppFrameBundle? Bundle, AppFrameFailure Failure)
{
    /// <summary>A failure.</summary>
    /// <param name="failure">Why.</param>
    /// <returns>The result.</returns>
    public static AppFrameBundleResult Refused(AppFrameFailure failure) => new(null, failure);
}
