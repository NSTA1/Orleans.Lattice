namespace Orleans.Lattice.Operations;

/// <summary>
/// The ambient <see cref="ILatticeOperationProgress"/> of the coordinated operation
/// running on the current logical call flow. Lets an engine behind a public
/// interface report progress without a signature change: it reads
/// <see cref="Current"/> once and reports only when it is not <see langword="null"/>,
/// so code running outside an operation pays a single null check.
/// </summary>
internal static class LatticeOperationProgress
{
    private static readonly AsyncLocal<ILatticeOperationProgress?> CurrentSink = new();

    /// <summary>The progress sink of the operation on this call flow, or <see langword="null"/>.</summary>
    public static ILatticeOperationProgress? Current => CurrentSink.Value;

    /// <summary>
    /// Makes <paramref name="progress"/> current until the returned scope is
    /// disposed. Pass <see langword="null"/> to suppress reporting for nested work
    /// whose own units would otherwise overwrite an outer phase.
    /// </summary>
    /// <param name="progress">The sink, or <see langword="null"/>.</param>
    /// <returns>A scope that restores the previous sink.</returns>
    public static Scope Enter(ILatticeOperationProgress? progress)
    {
        var previous = CurrentSink.Value;
        CurrentSink.Value = progress;
        return new Scope(previous);
    }

    /// <summary>Restores the previously current sink when disposed.</summary>
    public readonly struct Scope : IDisposable
    {
        private readonly ILatticeOperationProgress? _previous;

        internal Scope(ILatticeOperationProgress? previous) => _previous = previous;

        /// <inheritdoc />
        public void Dispose() => CurrentSink.Value = _previous;
    }
}
