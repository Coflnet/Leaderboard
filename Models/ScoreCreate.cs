namespace Coflnet.Leaderboard.Models;

public class ScoreCreate
{
    public string UserId { get; set; }
    public long Score { get; set; }
    public byte Confidence { get; set; }
    /// <summary>
    /// Only increase score if it is higher than the current one
    /// </summary>
    public bool HighScore { get; set; }
}