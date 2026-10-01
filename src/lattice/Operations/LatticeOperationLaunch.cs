namespace Orleans.Lattice.Operations;

/// <summary>The outcome of <see cref="LatticeOperationRunner.StartAsync"/>.</summary>
/// <typeparam name="TResult">The engine result type.</typeparam>
/// <param name="Record">The operation record as accepted.</param>
/// <param name="Completion">
/// The in-process task of the work when this call started it, which completes with
/// the engine's own result or faults with the engine's own exception; or
/// <see langword="null"/> when an operation with the same id already existed and
/// nothing was started.
/// </param>
internal sealed record LatticeOperationLaunch<TResult>(
    LatticeOperationRecord Record,
    Task<TResult>? Completion);
