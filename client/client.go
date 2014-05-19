package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/schmatz/coco-verify/lib"
	"labix.org/v2/mgo/bson"
)

type GameSessionResults struct {
	GameSessionPair lib.GameSessionPair
	Winner          string
}

func convertPairStringToGameSessionPair(p string) lib.GameSessionPair {
	var sessionPair lib.GameSessionPair
	fmt.Println(p[:23])
	sessionPair[0] = lib.GameSession{bson.ObjectIdHex(string(p[:24])), "humans"}
	sessionPair[1] = lib.GameSession{bson.ObjectIdHex(string(p[24:])), "ogres"}
	return sessionPair
}
func getGameSessionPairToProcess(r redis.Conn) lib.GameSessionPair {
	rawString, err := redis.String(r.Do("SPOP", lib.UnprocessedSetName))
	if err != nil {
		panic(err)
	}
	n, err := r.Do("SADD", lib.ProcessingName, rawString)
	if err != nil {
		panic(err)
	}
	fmt.Println("Moved", n, "members to processing state!")
	return convertPairStringToGameSessionPair(rawString)
}
func simulateGame(pairToSimulate lib.GameSessionPair) GameSessionResults {
	var results GameSessionResults
	results.GameSessionPair = pairToSimulate
	//do simulation here
	results.Winner = "ogres"
	return results
}

func addResultsToWinsAndLosses(resultString string, gameSessionPair lib.GameSessionPair, r redis.Conn) {
	var winningIndex, losingIndex int
	if resultString == "tie" {
		n, err := redis.Int(r.Do("SADD", gameSessionPair[0].GetLosingRedisKey(), gameSessionPair[1].ID.Hex()))
		if err != nil {
			panic(err)
		}
		n, err = redis.Int(r.Do("SADD", gameSessionPair[1].GetLosingRedisKey(), gameSessionPair[0].ID.Hex()))
		if err != nil {
			panic(err)
		}
		fmt.Println("Moved", n, "games to tie")

	} else {
		switch resultString {
		case "humans":
			winningIndex = 0
			losingIndex = 1
		case "ogres":
			winningIndex = 1
			losingIndex = 0
		}
		n, err := redis.Int(r.Do("SADD", gameSessionPair[winningIndex].GetWinningRedisKey(), gameSessionPair[losingIndex].ID.Hex()))
		if err != nil {
			panic(err)
		}
		fmt.Println("Added", n, "game to", gameSessionPair[winningIndex].GetWinningRedisKey())
		n, err = redis.Int(r.Do("SADD", gameSessionPair[losingIndex].GetLosingRedisKey(), gameSessionPair[winningIndex].ID.Hex()))
		if err != nil {
			panic(err)
		}
		fmt.Println("Added", n, "game to", gameSessionPair[losingIndex].GetLosingRedisKey())
	}

}
func recordResults(results GameSessionResults, r redis.Conn) {
	addResultsToWinsAndLosses(results.Winner, results.GameSessionPair, r)

	n, err := r.Do("SMOVE", lib.ProcessingName, lib.ProcessedSetName, results.GameSessionPair.RedisQueueKey())
	if err != nil {
		panic(err)
	}
	fmt.Println("Moved", n, "results to processed state!")

}
func processGame(r redis.Conn) {
	gameSessionPairToProcess := getGameSessionPairToProcess(r)
	results := simulateGame(gameSessionPairToProcess)
	recordResults(results, r)

}
func main() {
	//machineCores = runtime.NumCPU()
	r := lib.ConnectToRedis()
	processGame(r)

}
