namespace Orleans.Lattice.Apps;

/// <summary>
/// Why an <see cref="IAppActivationPipeline"/> run failed, or <see cref="None"/> when it
/// succeeded. Every failure is reported as a value, never as an exception into silo startup.
/// </summary>
[GenerateSerializer, Alias(AppActivationTypeAliases.AppActivationFailure)]
public enum AppActivationFailure
{
    /// <summary>The run succeeded.</summary>
    None = 0,

    /// <summary>The app has no registry record for the tenant.</summary>
    NotInstalled = 1,

    /// <summary>The app's registry state does not permit the requested operation.</summary>
    InvalidTransition = 2,

    /// <summary>The stored capability ceiling was not consented for the stored version.</summary>
    CeilingNotPinned = 3,

    /// <summary>
    /// Membership is not registered, so every caller is anonymous with no groups and every app
    /// rule would be unmatchable. The App to Auth to Membership chain is incomplete.
    /// </summary>
    MembershipNotRegistered = 4,

    /// <summary>The authorization policy store is not registered, so app rules cannot be persisted.</summary>
    AuthorizationNotRegistered = 5,

    /// <summary>The app source could not supply the app (not found, identity mismatch, or duplicate registration).</summary>
    SourceUnavailable = 6,

    /// <summary>The app source supplies a different version from the installed one.</summary>
    VersionMismatch = 7,

    /// <summary>The manifest failed to parse or validate.</summary>
    InvalidManifest = 8,

    /// <summary>A declared role requests operations or scopes beyond the consented capability ceiling.</summary>
    CeilingExceeded = 9,

    /// <summary>A stored role binding names a role the manifest does not declare.</summary>
    UnknownRoleBinding = 10,

    /// <summary>Creating, recovering, or soft-deleting one of the app's structural trees failed.</summary>
    TreeProvisioningFailed = 11,

    /// <summary>Writing or withdrawing the app's owned authorization rules failed.</summary>
    RulePersistenceFailed = 12,

    /// <summary>The registry record changed concurrently and the transition could not be applied.</summary>
    RegistryConflict = 13,

    /// <summary>An unexpected error occurred; the diagnostics carry its description.</summary>
    Faulted = 14,

    /// <summary>A declared replication mode differs from the previously enabled mode or is ambiguous.</summary>
    ReplicationModeChangeRejected = 15,

    /// <summary>Replication prerequisites, such as a configured cluster id, are not satisfied.</summary>
    ReplicationPreconditionFailed = 16,

    /// <summary>Reading or writing the runtime replication enrolment failed.</summary>
    ReplicationEnrolmentFailed = 17,
}
