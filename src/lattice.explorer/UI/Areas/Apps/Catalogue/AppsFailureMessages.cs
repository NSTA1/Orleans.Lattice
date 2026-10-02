namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Turns a failed facade call into a specific, human sentence. Raw exception text
/// never reaches the page: the facades' messages are only read to recognise which
/// documented failure occurred (the <c>AppsControlFailures</c> categories), and the
/// sentence shown is always one written here.
/// </summary>
internal static class AppsFailureMessages
{
    /// <summary>
    /// The sentence for a role bound to a group of another tenant, which the cluster
    /// refuses at install or re-binding, and again at activation for a stored one.
    /// </summary>
    public const string TenantMismatchSentence =
        "A role is bound to another tenant's group. Bind each role to a cluster group or to one of this tenant's own groups.";

    // How the apps registry words the same refusal when it rejects the request itself, before activation.
    private const string TenantMismatchArgumentToken = "which is not a group of the installing tenant";

    private static readonly (string Token, string Sentence)[] Known =
    [
        ("(CeilingExceeded)", "It asks for more than the approved ceiling. Re-consent to a ceiling that covers what it needs."),
        ("(BridgeConsentRequired)", "Its UI asks for bridge operations that were not consented. Review and re-consent them."),
        ("(TreeOwnershipConflict)", "One of its trees is already owned by another app or by the cluster, so it cannot take ownership."),
        ("(CeilingNotPinned)", "Its consent was not recorded for the installed version. Re-consent, then try again."),
        ("(UnknownRoleBinding)", "A role binding names a role this version no longer declares. Bind its roles again."),
        ("(AppRoleBindingTenantMismatch)", TenantMismatchSentence),
        ("(InvalidManifest)", "Its manifest did not validate, so nothing was changed."),
        ("(SourceUnavailable)", "Its source could not supply it. The source may be unreachable, or the app was withdrawn."),
        ("(VersionMismatch)", "Its source now offers a different version from the installed one."),
        ("(InvalidTransition)", "It is not in a state that allows this. Reload to see its current state."),
        ("(RegistryConflict)", "It changed while you were working. Reload and try again."),
        ("(ConcurrencyConflict)", "It changed while you were working. Reload and try again."),
        ("(TreeProvisioningFailed)", "One of its trees could not be created or recovered. Try again, or ask a cluster operator."),
        ("(RulePersistenceFailed)", "Its access rules could not be written. Try again, or ask a cluster operator."),
        ("(ReplicationModeChangeRejected)", "It changes the replication mode of a tree, which is not allowed."),
        ("(ReplicationPreconditionFailed)", "It needs replication, which this cluster is not configured for."),
        ("(ReplicationEnrolmentFailed)", "Its trees could not be enrolled for replication. Try again, or ask a cluster operator."),
        ("(MembershipNotRegistered)", "This cluster has no membership service, so app roles cannot be bound."),
        ("(AuthorizationNotRegistered)", "This cluster has no authorization policy store, so app rules cannot be written."),
        ("(Ambiguous)", "It is offered by more than one source. Install it from the source you choose."),
        ("offered by more than one app source", "It is offered by more than one source. Install it from the source you choose."),
        ("already installed at that version", "That version is already installed. Update its consent instead, or uninstall it first."),
        ("no longer matches the manifest that was reviewed", "Its source changed it after you reviewed it, so nothing was changed. Review it again."),
    ];

    /// <summary>The sentence for a failed call.</summary>
    /// <param name="error">The failure.</param>
    /// <param name="verb">What was being done, such as "install" or "enable".</param>
    /// <param name="app">The app's name as shown.</param>
    public static string Describe(Exception error, string verb, string app)
    {
        ArgumentNullException.ThrowIfNull(error);
        ArgumentException.ThrowIfNullOrWhiteSpace(verb);
        ArgumentException.ThrowIfNullOrWhiteSpace(app);

        var lead = $"Could not {verb} {app}.";
        return error switch
        {
            UnauthorizedAccessException => $"{lead} You are not allowed to {verb} apps here.",
            KeyNotFoundException => $"{lead} It is not installed here, or that version is no longer offered by its source.",
            OperationCanceledException => $"{lead} The request was cancelled.",
            NotSupportedException => $"{lead} This cluster does not serve app management.",
            ArgumentException argument when IsTenantMismatch(argument.Message) => $"{lead} {TenantMismatchSentence}",
            ArgumentException => $"{lead} The request was not valid: check its role bindings and ceiling.",
            InvalidOperationException invalid => $"{lead} {Recognise(invalid.Message)}",
            _ => $"{lead} The cluster could not be reached. Try again.",
        };
    }

    private static bool IsTenantMismatch(string? message) =>
        message is not null && message.Contains(TenantMismatchArgumentToken, StringComparison.Ordinal);

    private static string Recognise(string? message)
    {
        if (!string.IsNullOrEmpty(message))
        {
            foreach (var (token, sentence) in Known)
            {
                if (message.Contains(token, StringComparison.Ordinal))
                {
                    return sentence;
                }
            }
        }

        return "The cluster refused the change.";
    }
}
