using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo, live view of
/// <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/>: a
/// single <c>volatile</c> field read the hot paths consult (the active
/// <see cref="Orleans.Lattice.Auth.ITenantRuleLayer"/> and
/// <see cref="Orleans.Lattice.Membership.ITenantGroupClaimFilter"/> seams answer
/// <c>IsActive</c> from it), kept current through the options monitor, and a
/// <see cref="Changed"/> notification the compiled tenant-policy snapshot
/// maintainer uses to rebuild when the value flips.
/// </summary>
/// <remarks>
/// Reading <see cref="IsEnabled"/> is one field read and never allocates, which is
/// what the seams' <c>IsActive</c> contract requires. Only a change of the flag's
/// value raises <see cref="Changed"/>; an unrelated options reload does not.
/// </remarks>
internal sealed class DelegatedTenantAccessFlag : IDisposable
{
    private readonly IDisposable? _subscription;
    private readonly Lock _gate = new();
    private volatile bool _enabled;

    /// <summary>
    /// Initializes the flag from <paramref name="options"/> and subscribes to its
    /// changes.
    /// </summary>
    /// <param name="options">The tenancy options monitor. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    public DelegatedTenantAccessFlag(IOptionsMonitor<LatticeTenancyOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _enabled = options.CurrentValue.DelegatedAccessAdministrationEnabled;
        _subscription = options.OnChange(OnOptionsChanged);
    }

    /// <summary>Initializes a fixed flag that never changes. Used by tests and by hosts without an options monitor.</summary>
    /// <param name="enabled">The fixed value.</param>
    internal DelegatedTenantAccessFlag(bool enabled) => _enabled = enabled;

    /// <summary>A fixed flag that is always off: the posture of a silo that never enabled the feature.</summary>
    internal static DelegatedTenantAccessFlag Disabled { get; } = new(false);

    /// <summary>Raised, after <see cref="IsEnabled"/> has taken its new value, whenever the flag's value changes.</summary>
    public event Action? Changed;

    /// <summary>Whether delegated tenant access administration is currently enabled on this silo.</summary>
    public bool IsEnabled => _enabled;

    /// <summary>
    /// <see cref="IsEnabled"/> as a method, so it can be handed out as a
    /// <see cref="Func{TResult}"/> (for example to membership's tenant group claim
    /// filter) whose every invocation is one field read.
    /// </summary>
    /// <returns>The current value of <see cref="IsEnabled"/>.</returns>
    public bool ReadIsEnabled() => _enabled;

    /// <summary>
    /// Sets the flag directly and raises <see cref="Changed"/> when the value moves.
    /// The options-monitor path calls this; tests drive it to flip the flag
    /// deterministically.
    /// </summary>
    /// <param name="enabled">The new value.</param>
    internal void Set(bool enabled)
    {
        lock (_gate)
        {
            if (_enabled == enabled)
            {
                return;
            }

            _enabled = enabled;
        }

        Changed?.Invoke();
    }

    /// <inheritdoc />
    public void Dispose() => _subscription?.Dispose();

    private void OnOptionsChanged(LatticeTenancyOptions options, string? name)
    {
        // The tenancy options are unnamed; a reload of some other named instance
        // says nothing about this silo's posture.
        if (name is not null && !string.Equals(name, Options.DefaultName, StringComparison.Ordinal))
        {
            return;
        }

        Set(options.DelegatedAccessAdministrationEnabled);
    }
}
