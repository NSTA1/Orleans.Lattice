namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The cancellation a component, or a circuit-scoped service, hands to the work it starts:
/// one token for everything it reads, cancelled when it is left, and never disposed.
/// </summary>
/// <remarks>
/// <para>
/// A read is often already on its way when its component is left, and its continuation
/// resumes after <c>Dispose</c>. Disposing a <see cref="CancellationTokenSource"/> there
/// makes the next read of its <see cref="CancellationTokenSource.Token"/> throw
/// <see cref="ObjectDisposedException"/> out of a lifecycle method, and that ends the Blazor
/// circuit: the console stays on screen, inert (issue #4011). A plain source holds nothing
/// that needs disposing once it is cancelled, so this type cancels and never disposes, and
/// its members never throw for having been left.
/// </para>
/// <para>
/// A component checks <see cref="IsLeft"/> after an await before it does anything the page
/// being shown would see - declaring not-found, navigating, starting a flow, raising a
/// toast - because by then another page may be on screen.
/// </para>
/// <para>
/// Work that is replaced rather than ended - a reload, a follow, a completion query - takes
/// a fresh token from <see cref="Renew"/>, which cancels the work it replaces.
/// </para>
/// </remarks>
internal sealed class ComponentLifetime
{
    private readonly Lock _gate = new();
    private CancellationTokenSource _current = new();
    private bool _left;

    /// <summary>
    /// The token of the current work: cancelled when it is renewed or the owner is left.
    /// Reading it never throws.
    /// </summary>
    public CancellationToken Token
    {
        get
        {
            lock (_gate)
            {
                return _current.Token;
            }
        }
    }

    /// <summary>Whether the owner has been left; once true it stays true.</summary>
    public bool IsLeft
    {
        get
        {
            lock (_gate)
            {
                return _left;
            }
        }
    }

    /// <summary>
    /// Cancels the current work and returns the token for the work that replaces it; once
    /// the owner has been left, returns an already cancelled token.
    /// </summary>
    /// <returns>The token for the next piece of work.</returns>
    public CancellationToken Renew()
    {
        CancellationTokenSource replaced;
        CancellationToken next;
        lock (_gate)
        {
            if (_left)
            {
                return _current.Token;
            }

            replaced = _current;
            _current = new CancellationTokenSource();
            next = _current.Token;
        }

        replaced.Cancel();
        return next;
    }

    /// <summary>Cancels everything the owner started, for good. Calling it again does nothing.</summary>
    public void Leave()
    {
        CancellationTokenSource current;
        lock (_gate)
        {
            if (_left)
            {
                return;
            }

            _left = true;
            current = _current;
        }

        current.Cancel();
    }
}
