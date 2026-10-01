using System.Globalization;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The circuit's staged backup operations. An operation runs independently of
/// the page that started it, so the user can leave its status page and come
/// back to it; ending the circuit cancels whatever is still running.
/// </summary>
/// <remarks>
/// An operation belongs to the tenant the circuit asserted when it started: its
/// work runs pinned to that tenant, so a tenant switch part-way through cannot
/// send its later calls to another tenant, and only the operations of the tenant
/// the circuit asserts now are listed or found.
/// </remarks>
internal sealed class BackupOperations : IDisposable
{
    private readonly TimeProvider _time;
    private readonly ShellAssertedTenant _tenant;
    private readonly ComponentLifetime _lifetime = new();
    private readonly object _gate = new();
    private readonly List<(BackupOperation Operation, string? Tenant)> _operations = [];
    private int _next;

    /// <summary>Creates the circuit's operation list.</summary>
    /// <param name="time">The clock operation times are read from.</param>
    /// <param name="tenant">The circuit's asserted tenant, which each operation is started in and pinned to.</param>
    public BackupOperations(TimeProvider time, ShellAssertedTenant? tenant = null)
    {
        ArgumentNullException.ThrowIfNull(time);
        _time = time;
        _tenant = tenant ?? ShellAssertedTenant.None;
    }

    /// <summary>Every operation started in this circuit under the tenant it asserts now, newest first.</summary>
    public IReadOnlyList<BackupOperation> Recent
    {
        get
        {
            var tenant = _tenant.AssertedTenant;
            lock (_gate)
            {
                var recent = new List<BackupOperation>(_operations.Count);
                for (var i = _operations.Count - 1; i >= 0; i--)
                {
                    if (ShellAssertedTenant.Same(_operations[i].Tenant, tenant))
                    {
                        recent.Add(_operations[i].Operation);
                    }
                }

                return recent;
            }
        }
    }

    /// <summary>
    /// Starts an operation and returns it at once, running <paramref name="work"/>
    /// in the background. The work advances the operation through its stages and
    /// may report results; a fault fails it with a plain sentence, and returning
    /// without finishing it succeeds it.
    /// </summary>
    /// <param name="kind">What it does.</param>
    /// <param name="title">Its title.</param>
    /// <param name="stages">Its stages, in order.</param>
    /// <param name="work">The work, given the operation and the circuit's lifetime token.</param>
    /// <param name="reverts">For a revert, the cluster operation id of the restore it reverts.</param>
    public BackupOperation Start(
        BackupOperationKind kind,
        string title,
        IReadOnlyList<string> stages,
        Func<BackupOperation, CancellationToken, Task> work,
        string? reverts = null)
    {
        ArgumentNullException.ThrowIfNull(work);
        ObjectDisposedException.ThrowIf(_lifetime.IsLeft, this);

        var tenant = _tenant.AssertedTenant;
        BackupOperation operation;
        lock (_gate)
        {
            _next++;
            operation = new BackupOperation(_next.ToString(CultureInfo.InvariantCulture), kind, title, stages, _time, reverts);
            _operations.Add((operation, tenant));
        }

        _ = RunAsync(operation, work, _tenant, tenant, _lifetime.Token);
        return operation;
    }

    /// <summary>The operation with <paramref name="id"/> under the tenant the circuit asserts now, or <see langword="null"/>.</summary>
    /// <param name="id">The operation id.</param>
    public BackupOperation? Find(string? id)
    {
        if (string.IsNullOrEmpty(id))
        {
            return null;
        }

        var tenant = _tenant.AssertedTenant;
        lock (_gate)
        {
            foreach (var (operation, owner) in _operations)
            {
                if (string.Equals(operation.Id, id, StringComparison.Ordinal) && ShellAssertedTenant.Same(owner, tenant))
                {
                    return operation;
                }
            }

            return null;
        }
    }

    /// <summary>The most recent operation of <paramref name="kind"/> under the tenant the circuit asserts now, or <see langword="null"/>.</summary>
    /// <param name="kind">The kind.</param>
    public BackupOperation? Latest(BackupOperationKind kind)
    {
        var tenant = _tenant.AssertedTenant;
        lock (_gate)
        {
            for (var i = _operations.Count - 1; i >= 0; i--)
            {
                var (operation, owner) = _operations[i];
                if (operation.Kind == kind && ShellAssertedTenant.Same(owner, tenant))
                {
                    return operation;
                }
            }

            return null;
        }
    }

    /// <summary>
    /// The most recent revert of the cluster restore <paramref name="clusterOperationId"/>
    /// started in this circuit under the tenant it asserts now, or <see langword="null"/>.
    /// A failed revert is skipped, so the restore can be reverted again.
    /// </summary>
    /// <param name="clusterOperationId">The restore's cluster operation id.</param>
    public BackupOperation? RevertOf(string? clusterOperationId)
    {
        if (string.IsNullOrEmpty(clusterOperationId))
        {
            return null;
        }

        var tenant = _tenant.AssertedTenant;
        lock (_gate)
        {
            for (var i = _operations.Count - 1; i >= 0; i--)
            {
                var (operation, owner) = _operations[i];
                if (string.Equals(operation.Reverts, clusterOperationId, StringComparison.Ordinal)
                    && operation.Status is not (BackupOperationStatus.Failed or BackupOperationStatus.Cancelled)
                    && ShellAssertedTenant.Same(owner, tenant))
                {
                    return operation;
                }
            }

            return null;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
    }

    private static async Task RunAsync(
        BackupOperation operation,
        Func<BackupOperation, CancellationToken, Task> work,
        ShellAssertedTenant asserted,
        string? tenant,
        CancellationToken lifetime)
    {
        // The pin lives in this method's own execution context, so it holds for
        // every call the work makes and never leaks back to the caller.
        using var pin = asserted.Pin(tenant);
        try
        {
            await work(operation, lifetime).ConfigureAwait(false);
            operation.Succeed("Done.");
        }
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested)
        {
            operation.Cancel();
        }
        catch (Exception exception)
        {
            operation.Fail(BackupsFaults.Describe(exception));
        }
    }
}
