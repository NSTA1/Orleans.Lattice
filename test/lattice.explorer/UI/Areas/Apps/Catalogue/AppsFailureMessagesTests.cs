using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>Every failure maps to a specific, human sentence; raw exception text never shows.</summary>
[TestFixture]
public sealed class AppsFailureMessagesTests
{
    private const string Secret = "a/task-board/tasks@t-acme#physical-7f3e";

    [TestCase("The enable of app 'task-board' failed (CeilingExceeded): " + Secret, "more than the approved ceiling")]
    [TestCase("The enable of app 'task-board' failed (BridgeConsentRequired).", "bridge operations that were not consented")]
    [TestCase("Could not install app 'task-board' (TreeOwnershipConflict): " + Secret, "already owned")]
    [TestCase("The enable of app 'x' failed (CeilingNotPinned).", "not recorded for the installed version")]
    [TestCase("The enable of app 'x' failed (UnknownRoleBinding).", "Bind its roles again")]
    [TestCase("The install of app 'x' failed (InvalidManifest).", "did not validate")]
    [TestCase("The enable of app 'x' failed (SourceUnavailable).", "source could not supply")]
    [TestCase("The enable of app 'x' failed (VersionMismatch).", "different version")]
    [TestCase("Could not enable app 'x' (InvalidTransition).", "not in a state")]
    [TestCase("The enable of app 'x' failed (RegistryConflict).", "changed while you were working")]
    [TestCase("Could not install app 'x' (ConcurrencyConflict).", "changed while you were working")]
    [TestCase("The enable of app 'x' failed (TreeProvisioningFailed).", "could not be created")]
    [TestCase("The enable of app 'x' failed (RulePersistenceFailed).", "access rules could not be written")]
    [TestCase("The enable of app 'x' failed (ReplicationModeChangeRejected).", "replication mode")]
    [TestCase("The enable of app 'x' failed (ReplicationPreconditionFailed).", "not configured for")]
    [TestCase("The enable of app 'x' failed (ReplicationEnrolmentFailed).", "enrolled for replication")]
    [TestCase("The enable of app 'x' failed (MembershipNotRegistered).", "no membership service")]
    [TestCase("The enable of app 'x' failed (AuthorizationNotRegistered).", "no authorization policy store")]
    [TestCase("App 'x' is offered by more than one app source (a, b); name the source key to use.", "more than one source")]
    [TestCase("App 'x' is already installed at that version; update its consent instead.", "already installed")]
    [TestCase("App 'x' version '1.0.0' no longer matches the manifest that was reviewed; review it again before installing.", "Review it again")]
    [TestCase("something new: " + Secret, "The cluster refused the change.")]
    public void An_invalid_operation_is_recognised_by_its_documented_category(string message, string expected)
    {
        var sentence = AppsFailureMessages.Describe(new InvalidOperationException(message), "install", "Task board");

        Assert.Multiple(() =>
        {
            Assert.That(sentence, Does.StartWith("Could not install Task board."));
            Assert.That(sentence, Does.Contain(expected));
            Assert.That(sentence, Does.Not.Contain(Secret), "raw exception text never reaches the page");
        });
    }

    [Test]
    public void Every_other_failure_kind_has_its_own_sentence()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsFailureMessages.Describe(new LatticeAuthorizationDeniedException(Secret), "enable", "CRM"), Is.EqualTo("Could not enable CRM. You are not allowed to enable apps here."));
            Assert.That(AppsFailureMessages.Describe(new KeyNotFoundException(Secret), "enable", "CRM"), Does.Contain("not installed here"));
            Assert.That(AppsFailureMessages.Describe(new OperationCanceledException(), "enable", "CRM"), Does.Contain("cancelled"));
            Assert.That(AppsFailureMessages.Describe(new NotSupportedException(Secret), "enable", "CRM"), Does.Contain("does not serve app management"));
            Assert.That(AppsFailureMessages.Describe(new ArgumentException(Secret), "enable", "CRM"), Does.Contain("not valid"));
            Assert.That(AppsFailureMessages.Describe(new TimeoutException(Secret), "enable", "CRM"), Does.Contain("could not be reached"));
            Assert.That(AppsFailureMessages.Describe(new InvalidOperationException(), "enable", "CRM"), Does.Contain("refused"));
            Assert.That(() => AppsFailureMessages.Describe(null!, "enable", "CRM"), Throws.ArgumentNullException);
        });
    }
}
