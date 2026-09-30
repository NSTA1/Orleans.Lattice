namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Lifecycle actions a palette command asked for, waiting for the page that shows
/// their visible control. The palette navigates to the command's target and then
/// posts the intent; the page takes it and opens the same confirmation its own
/// button opens, so a command never skips a confirmation.
/// </summary>
internal sealed class AppsLifecycleIntents
{
    private readonly Dictionary<string, AppLifecycleVerb> _pending = new(StringComparer.Ordinal);

    /// <summary>Raised when an intent is posted, so a page already showing the app can take it.</summary>
    public event Action? Posted;

    /// <summary>Asks for <paramref name="verb"/> on <paramref name="slug"/>, replacing any earlier request for it.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="verb">The action.</param>
    public void Post(string slug, AppLifecycleVerb verb)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(slug);
        _pending[slug] = verb;
        Posted?.Invoke();
    }

    /// <summary>Takes the pending action for <paramref name="slug"/> when it is <paramref name="verb"/>.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="verb">The action the caller handles.</param>
    /// <returns>Whether that action was pending; any other pending action is left for its own handler.</returns>
    public bool TryTake(string slug, AppLifecycleVerb verb)
    {
        if (_pending.TryGetValue(slug, out var pending) && pending == verb)
        {
            _pending.Remove(slug);
            return true;
        }

        return false;
    }
}
