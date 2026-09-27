using System.Reflection;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The in-image activation handle. The app's code is already present through ordinary package
/// reference, so activation loads nothing and reports the registered assembly.
/// </summary>
internal sealed class InImageAppActivationHandle(AppIdentity identity, Assembly assembly) : IAppActivationHandle
{
    private AppActivationResult? result;

    /// <inheritdoc />
    public AppIdentity Identity { get; } = identity;

    /// <inheritdoc />
    public ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default)
    {
        var activated = Volatile.Read(ref result);
        if (activated is null)
        {
            Interlocked.CompareExchange(ref result, AppActivationResult.Activated(assembly), null);
            activated = result;
        }

        return new(activated);
    }
}
