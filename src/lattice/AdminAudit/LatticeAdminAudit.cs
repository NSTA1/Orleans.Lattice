namespace Orleans.Lattice;

/// <summary>
/// The admin audit protocol every administration facade follows (#3638). It is
/// the single place the ordering lives, so a facade cannot get it wrong:
/// <list type="number">
/// <item><description><see cref="AuthorizeAsync"/>: a refusal writes one
/// <see cref="LatticeAdminAuditOutcome.Denied"/> record <b>before</b> the refusal is
/// rethrown.</description></item>
/// <item><description><see cref="RunAsync{T}"/>: the intent is written <b>before</b>
/// the commit and a failed intent refuses the call (fail closed); the commit runs;
/// the intent is finalized with the observed outcome. A failed finalize is left to
/// the reconciler.</description></item>
/// </list>
/// With no trail registered, or the trail disabled, both run the delegate alone.
/// </summary>
internal static class LatticeAdminAudit
{
    /// <summary>
    /// Runs a facade's authorization step; when it refuses, records the denial and
    /// rethrows the refusal unchanged.
    /// </summary>
    /// <param name="trail">The audit trail, or <see langword="null"/> when none is registered.</param>
    /// <param name="request">The call being authorized.</param>
    /// <param name="authorize">The authorization step; it throws to refuse.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    public static async ValueTask AuthorizeAsync(
        ILatticeAdminAuditTrail? trail,
        LatticeAdminAuditRequest request,
        Func<CancellationToken, ValueTask> authorize,
        CancellationToken cancellationToken)
    {
        if (trail is null || !trail.IsEnabled)
        {
            await authorize(cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await authorize(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (Classify(ex).Outcome == LatticeAdminAuditOutcome.Denied)
        {
            await trail.RecordDeniedAsync(request, Classify(ex).ReasonCode!, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs a facade's authorization step that also returns a value (for example the
    /// record the authorization read); when it refuses, records the denial and rethrows
    /// the refusal unchanged.
    /// </summary>
    /// <typeparam name="T">The authorization step's result.</typeparam>
    /// <param name="trail">The audit trail, or <see langword="null"/> when none is registered.</param>
    /// <param name="request">The call being authorized.</param>
    /// <param name="authorize">The authorization step; it throws to refuse.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>The authorization step's result.</returns>
    public static async Task<T> AuthorizeAsync<T>(
        ILatticeAdminAuditTrail? trail,
        LatticeAdminAuditRequest request,
        Func<CancellationToken, ValueTask<T>> authorize,
        CancellationToken cancellationToken)
    {
        if (trail is null || !trail.IsEnabled)
        {
            return await authorize(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            return await authorize(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (Classify(ex).Outcome == LatticeAdminAuditOutcome.Denied)
        {
            await trail.RecordDeniedAsync(request, Classify(ex).ReasonCode!, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs an audited mutation: intent, commit, finalize.
    /// </summary>
    /// <typeparam name="T">The commit's result.</typeparam>
    /// <param name="trail">The audit trail, or <see langword="null"/> when none is registered.</param>
    /// <param name="request">The call.</param>
    /// <param name="commit">The facade's validate-and-commit step.</param>
    /// <param name="changed">
    /// Reports from the result whether the call changed state, or
    /// <see langword="null"/> when every successful return is a change.
    /// </param>
    /// <param name="cancellationToken">Cancels the intent write and the commit; the finalize is not cancellable.</param>
    /// <param name="undecided">
    /// Reports from the result that the facade returned before the change settled (for
    /// example an accepted, still-running activation), so the intent is left
    /// <see cref="LatticeAdminAuditOutcome.Pending"/> for the reconciler; or
    /// <see langword="null"/> when every result is settled.
    /// </param>
    /// <returns>The commit's result.</returns>
    /// <exception cref="LatticeAdminAuditUnavailableException">The intent could not be written; nothing was attempted.</exception>
    public static async Task<T> RunAsync<T>(
        ILatticeAdminAuditTrail? trail,
        LatticeAdminAuditRequest request,
        Func<CancellationToken, Task<T>> commit,
        Func<T, bool>? changed,
        CancellationToken cancellationToken,
        Func<T, bool>? undecided = null)
    {
        if (trail is null || !trail.IsEnabled)
        {
            return await commit(cancellationToken).ConfigureAwait(false);
        }

        var intent = await trail.BeginAsync(request, cancellationToken).ConfigureAwait(false);
        T result;
        try
        {
            result = await commit(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await trail.CompleteAsync(intent, Classify(ex)).ConfigureAwait(false);
            throw;
        }

        var completion = undecided is not null && undecided(result)
            ? new LatticeAdminAuditCompletion(LatticeAdminAuditOutcome.Pending, null)
            : changed is null || changed(result)
                ? LatticeAdminAuditCompletion.Committed
                : LatticeAdminAuditCompletion.NoChange;
        await trail.CompleteAsync(intent, completion).ConfigureAwait(false);
        return result;
    }

    /// <summary>Runs an audited mutation that returns nothing; every successful return is a change.</summary>
    /// <param name="trail">The audit trail, or <see langword="null"/> when none is registered.</param>
    /// <param name="request">The call.</param>
    /// <param name="commit">The facade's validate-and-commit step.</param>
    /// <param name="cancellationToken">Cancels the intent write and the commit; the finalize is not cancellable.</param>
    /// <exception cref="LatticeAdminAuditUnavailableException">The intent could not be written; nothing was attempted.</exception>
    public static Task RunAsync(
        ILatticeAdminAuditTrail? trail,
        LatticeAdminAuditRequest request,
        Func<CancellationToken, Task> commit,
        CancellationToken cancellationToken) =>
        RunAsync<bool>(
            trail,
            request,
            async ct =>
            {
                await commit(ct).ConfigureAwait(false);
                return true;
            },
            changed: null,
            cancellationToken);

    /// <summary>
    /// Records an authorized call that failed before it attempted any change (a
    /// validation step that runs ahead of the commit): a refusal as
    /// <see cref="LatticeAdminAuditOutcome.Denied"/>, a domain refusal as
    /// <see cref="LatticeAdminAuditOutcome.Failed"/>. Never throws: the caller rethrows
    /// its own failure, which is what its client sees.
    /// </summary>
    /// <param name="trail">The audit trail, or <see langword="null"/> when none is registered.</param>
    /// <param name="request">The call.</param>
    /// <param name="failure">The failure the call is about to rethrow.</param>
    public static async Task RecordFailureAsync(
        ILatticeAdminAuditTrail? trail,
        LatticeAdminAuditRequest request,
        Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        if (trail is null || !trail.IsEnabled)
        {
            return;
        }

        var completion = Classify(failure);
        switch (completion.Outcome)
        {
            case LatticeAdminAuditOutcome.Denied:
                await trail.RecordDeniedAsync(request, completion.ReasonCode!, CancellationToken.None).ConfigureAwait(false);
                return;
            case LatticeAdminAuditOutcome.Failed:
                try
                {
                    var intent = await trail.BeginAsync(request, CancellationToken.None).ConfigureAwait(false);
                    await trail.CompleteAsync(intent, completion).ConfigureAwait(false);
                }
                catch (LatticeAdminAuditUnavailableException)
                {
                    // The trail already counted and logged the lost write; the caller's
                    // own failure is what its client must see.
                }

                return;
            default:
                // An infrastructure failure in a read-only step: nothing to attribute.
                return;
        }
    }

    /// <summary>
    /// Maps a failure to its audit outcome and reason code. Refusals by
    /// authorization or tier confinement are <see cref="LatticeAdminAuditOutcome.Denied"/>;
    /// deliberate domain refusals (validation, conflict, quota, not found), which the
    /// stores raise before writing, are <see cref="LatticeAdminAuditOutcome.Failed"/>;
    /// any other failure (a timeout, a cancellation, a storage or transport fault, a
    /// framework exception) may have struck after the commit landed, so it is
    /// <see cref="LatticeAdminAuditOutcome.Pending"/>, left for the reconciler to
    /// resolve from evidence. The exception message is never recorded.
    /// </summary>
    /// <param name="exception">The failure.</param>
    /// <returns>The completion to record.</returns>
    internal static LatticeAdminAuditCompletion Classify(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (exception is OperationCanceledException or TimeoutException
            or LatticeStateWriteFailedException or LatticeSaturatedException or ILatticeLeafUnavailable)
        {
            return new(LatticeAdminAuditOutcome.Pending, null);
        }

        if (exception is UnauthorizedAccessException)
        {
            return new(LatticeAdminAuditOutcome.Denied, LatticeAdminAuditReasonCodes.DeniedNotAuthorized);
        }

        // The tenancy, membership, auth and apps refusals live in packages the core
        // cannot reference, so they are recognised by their stable public type names.
        var type = exception.GetType();
        var name = type.Name;
        if (Has(name, "TenantOwned") || Has(name, "AppOwned") || Has(name, "Confinement"))
        {
            return new(LatticeAdminAuditOutcome.Denied, LatticeAdminAuditReasonCodes.DeniedTenantOwned);
        }

        if (Has(name, "TenantScopeRequired"))
        {
            return new(LatticeAdminAuditOutcome.Denied, LatticeAdminAuditReasonCodes.DeniedTenantScope);
        }

        if (Has(name, "Denied") || Has(name, "AdministrationDisabled"))
        {
            return new(LatticeAdminAuditOutcome.Denied, LatticeAdminAuditReasonCodes.DeniedNotAuthorized);
        }

        if (exception is LatticeQuotaExceededException || Has(name, "Quota"))
        {
            return new(LatticeAdminAuditOutcome.Failed, LatticeAdminAuditReasonCodes.FailedQuota);
        }

        if (exception is KeyNotFoundException || Has(name, "NotFound"))
        {
            return new(LatticeAdminAuditOutcome.Failed, LatticeAdminAuditReasonCodes.FailedNotFound);
        }

        if (Has(name, "Concurrency") || Has(name, "Conflict") || Has(name, "AlreadyExists")
            || Has(name, "Transition") || Has(name, "Last"))
        {
            return new(LatticeAdminAuditOutcome.Failed, LatticeAdminAuditReasonCodes.FailedConflict);
        }

        if (exception is ArgumentException or ILatticeDomainFault
            || (type.Namespace?.StartsWith("Orleans.Lattice", StringComparison.Ordinal) ?? false))
        {
            return new(LatticeAdminAuditOutcome.Failed, LatticeAdminAuditReasonCodes.FailedValidation);
        }

        return new(LatticeAdminAuditOutcome.Pending, null);

        static bool Has(string name, string part) => name.Contains(part, StringComparison.Ordinal);
    }
}
