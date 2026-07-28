using System;

namespace AzsbEmu.Server.Core;

public class MessageLockState
{
    public Guid LockToken { get; }
    public long SequenceNumber { get; }
    public long LogAddress { get; }
    public DateTimeOffset LockedUntil { get; set; }
    public string EntityPath { get; }
    public bool IsDlq { get; }
    public string LinkName { get; }

    public MessageLockState(Guid lockToken, long sequenceNumber, long logAddress, DateTimeOffset lockedUntil, string entityPath, bool isDlq, string linkName)
    {
        LockToken = lockToken;
        SequenceNumber = sequenceNumber;
        LogAddress = logAddress;
        LockedUntil = lockedUntil;
        EntityPath = entityPath;
        IsDlq = isDlq;
        LinkName = linkName;
    }
}
