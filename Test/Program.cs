using Coflnet.Leaderboard.Client.Api;

var boardName = "test-loadtest";
var client = new ScoresApi("http://localhost:5042");
await Parallel.ForEachAsync(Enumerable.Range(-10, 100), async (i, cancel) =>
{
    await client.ScoresLeaderboardSlugPostAsync(boardName, new()
    {
        Score = i,
        UserId = i.ToString()
    });
    await client.ScoresLeaderboardSlugPostAsync(boardName, new()
    {
        Score = i + 1,
        UserId = i.ToString()
    });
});

var scores = await client.ScoresLeaderboardSlugGetAsync(boardName);
scores = await client.ScoresLeaderboardSlugGetAsync(boardName);
Console.WriteLine($"Topscore: {scores[1].Score} from {scores[1].UserId} (should be 98)");
if(scores[0].UserId != "99" || scores[1].UserId != "98")
{
    Console.WriteLine("Test failed");
    return;
}


Console.WriteLine("Test passed");