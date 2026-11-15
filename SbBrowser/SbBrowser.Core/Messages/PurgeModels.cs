namespace SbBrowser.Core.Messages;

/// <summary>How far a purge has got: the messages removed so far.</summary>
public sealed record PurgeProgress(long Removed);

/// <summary>
/// What a purge did. <see cref="ScheduledRemaining"/> counts scheduled messages, which a purge leaves alone (they are
/// not receivable until their time comes). <see cref="DeferredRemaining"/> counts what is still in the purged queue
/// afterwards: deferred messages, which only a receive by sequence number can remove, plus any message that arrived
/// during the purge or sat in a session another receiver held locked.
/// </summary>
public sealed record PurgeResult(long Removed, long ScheduledRemaining, long DeferredRemaining, bool Cancelled);
