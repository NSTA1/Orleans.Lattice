namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The debounce seam for the Access-area subject picker's typeahead: coalesces a
/// rapid burst of keystrokes into a single deferred directory search, so the
/// picker issues one <see cref="IMembershipAdminService.SearchDirectoryAsync"/>
/// query per settle rather than one per character. The production implementation
/// defers the action behind a timer; a test double can capture and run the
/// pending action synchronously, letting the picker's debounce behaviour be
/// verified deterministically without any wall-clock delay.
/// </summary>
public interface ISubjectSearchDebounce
{
    /// <summary>
    /// Schedules <paramref name="action"/> to run after the debounce interval,
    /// superseding (cancelling) any previously scheduled action that has not yet
    /// run so only the most recent burst survives.
    /// </summary>
    /// <param name="action">The deferred search action. Must not be <see langword="null"/>.</param>
    void Schedule(Func<Task> action);
}
