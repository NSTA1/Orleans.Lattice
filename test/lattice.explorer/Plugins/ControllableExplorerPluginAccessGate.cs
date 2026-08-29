using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// An <see cref="IExplorerPluginAccessGate"/> whose completion the test drives
/// explicitly. Nothing here waits on a clock: a gate either answers
/// synchronously, throws synchronously, or stays pending until the test
/// completes its <see cref="TaskCompletionSource{TResult}"/>.
/// </summary>
internal sealed class ControllableExplorerPluginAccessGate : IExplorerPluginAccessGate
{
    private readonly TaskCompletionSource<ExplorerPluginAccess> _pending =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly ExplorerPluginAccess? _immediate;
    private readonly Exception? _throws;
    private readonly bool _hangs;

    private ControllableExplorerPluginAccessGate(
        ExplorerPluginAccess? immediate,
        Exception? throws,
        bool hangs)
    {
        _immediate = immediate;
        _throws = throws;
        _hangs = hangs;
    }

    /// <summary>The context this gate was probed with, or <see langword="null"/> when never probed.</summary>
    public IExplorerPluginHostContext? ObservedContext { get; private set; }

    /// <summary>How many times this gate was probed.</summary>
    public int ProbeCount { get; private set; }

    /// <summary>A gate that answers <paramref name="access"/> synchronously.</summary>
    public static ControllableExplorerPluginAccessGate Answering(ExplorerPluginAccess access) =>
        new(access, throws: null, hangs: false);

    /// <summary>A gate that throws <paramref name="fault"/> synchronously.</summary>
    public static ControllableExplorerPluginAccessGate Throwing(Exception fault) =>
        new(immediate: null, fault, hangs: false);

    /// <summary>
    /// A gate that never completes until <see cref="Complete"/> is called, so a
    /// test can assert what happens while one probe is outstanding without
    /// waiting on a clock.
    /// </summary>
    public static ControllableExplorerPluginAccessGate Hanging() =>
        new(immediate: null, throws: null, hangs: true);

    /// <summary>Completes a hanging probe with <paramref name="access"/>.</summary>
    public void Complete(ExplorerPluginAccess access) => _pending.TrySetResult(access);

    /// <summary>Faults a hanging probe with <paramref name="fault"/>.</summary>
    public void Fault(Exception fault) => _pending.TrySetException(fault);

    public ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default)
    {
        ProbeCount++;
        ObservedContext = context;

        if (_throws is not null)
        {
            throw _throws;
        }

        if (_hangs)
        {
            return new ValueTask<ExplorerPluginAccess>(_pending.Task);
        }

        return ValueTask.FromResult(_immediate ?? ExplorerPluginAccess.Denied);
    }
}
