using Orleans.Lattice.Explorer.UI.Areas.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>A page-visibility observer the test flips by hand.</summary>
internal sealed class FakePageVisibility : IReplicationPageVisibility
{
    public bool IsVisible { get; private set; } = true;

    public int Starts { get; private set; }

    public int Subscribers => Changed?.GetInvocationList().Length ?? 0;

    public event Action? Changed;

    public ValueTask StartAsync()
    {
        Starts++;
        return ValueTask.CompletedTask;
    }

    public void Set(bool visible)
    {
        if (IsVisible == visible)
        {
            return;
        }

        IsVisible = visible;
        Changed?.Invoke();
    }
}
