namespace Coflnet.Leaderboard.Models;

public class ScoreCreate
{
    public string UserId { get; set; }
    public long Score { get; set; }
    public byte Confidence { get; set; }
}