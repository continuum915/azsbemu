using Amqp;
using Amqp.Types;

namespace AzsbEmu.Server.Core;

public class TransactionState
{
    public byte[] TxnId { get; }
    public DateTime LastActive { get; set; }
    public List<BufferedSend> Sends { get; } = new();
    public List<BufferedSettlement> Settlements { get; } = new();
    public long TotalSize { get; set; }

    public TransactionState(byte[] txnId)
    {
        TxnId = txnId;
        LastActive = DateTime.UtcNow;
    }
}

public class BufferedSend
{
    public string EntityPath { get; }
    public Message Message { get; }
    public bool IsDlq { get; }
    public int Size { get; }

    public BufferedSend(string entityPath, Message message, bool isDlq, int size)
    {
        EntityPath = entityPath;
        Message = message;
        IsDlq = isDlq;
        Size = size;
    }
}

public class BufferedSettlement
{
    public Guid LockToken { get; }
    public object Outcome { get; } // Amqp.Framing.Outcome or string dispositionStatus for management
    public string? DeadLetterReason { get; }
    public string? DeadLetterDescription { get; }

    public BufferedSettlement(Guid lockToken, object outcome, string? dlReason = null, string? dlDescription = null)
    {
        LockToken = lockToken;
        Outcome = outcome;
        DeadLetterReason = dlReason;
        DeadLetterDescription = dlDescription;
    }
}
