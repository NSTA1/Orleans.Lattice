namespace Orleans.Lattice.Explorer.UI.Authentication;

/// <summary>
/// Cascading UI state that lets any component request the login dialog be opened.
/// Owned by the layout and cascaded to the navigation panel's sign-in button and
/// the connection banner's "Sign in" action, so an authentication failure
/// anywhere can surface the same dialog.
/// </summary>
public sealed class LoginDialogState
{
    /// <summary><see langword="true"/> when the login dialog should be shown.</summary>
    public bool IsVisible { get; private set; }

    /// <summary>Raised whenever <see cref="IsVisible"/> changes.</summary>
    public event Action? Changed;

    /// <summary>Requests that the login dialog open.</summary>
    public void Open()
    {
        if (IsVisible)
        {
            return;
        }

        IsVisible = true;
        Changed?.Invoke();
    }

    /// <summary>Closes the login dialog.</summary>
    public void Close()
    {
        if (!IsVisible)
        {
            return;
        }

        IsVisible = false;
        Changed?.Invoke();
    }
}
