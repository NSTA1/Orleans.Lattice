namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>The outcome of the per-launch workspace gate.</summary>
/// <param name="Launch">The authorised launch, or <see langword="null"/> on failure.</param>
/// <param name="Failure">Why the gate refused; meaningful only when <paramref name="Launch"/> is <see langword="null"/>.</param>
internal readonly record struct AppFrameLaunchResult(AppFrameLaunch? Launch, AppFrameFailure Failure)
{
    /// <summary>A refusal.</summary>
    /// <param name="failure">Why.</param>
    /// <returns>The result.</returns>
    public static AppFrameLaunchResult Refused(AppFrameFailure failure) => new(null, failure);
}
