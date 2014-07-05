package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/schmatz/coco-verify/lib"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

const numberOfTopGamesToRank = 500

func ConnectToMongoAndGetCollection() *mgo.Collection {
	connectionURL := "mongodb://" + lib.MongoUsername + ":" + lib.MongoPassword + "@" + lib.MongoURL + ":27017/" + lib.DatabaseName + "?authSource=admin"
	mongoSession, err := mgo.Dial(connectionURL)
	if err != nil {
		panic(err)
	}
	c := mongoSession.DB(lib.DatabaseName).C(lib.CollectionName)
	fmt.Println("Connected to collection", lib.CollectionName)
	return c
}

func GetAllRelevantSessions(levelSessionsCollection *mgo.Collection) (topHumanSessions, topOgreSessions []lib.GameSession) {
	teams := [2]string{"humans", "ogres"}
	for _, teamName := range teams {
		queryParameters := bson.M{"level.original": "53558b5a9914f5a90d7ccddb", "submitted": true, "team": teamName}
		selection := bson.M{"team": 1, "totalScore": 1}
		sort := bson.M{"totalScore": -1}
		pipe := levelSessionsCollection.Pipe([]bson.M{{"$match": queryParameters}, {"$project": selection}, {"$sort": sort}, {"$limit": numberOfTopGamesToRank}})
		var err error
		var documentCount int
		if teamName == "humans" {
			err = pipe.All(&topHumanSessions)
			documentCount = len(topHumanSessions)
		} else {
			err = pipe.All(&topOgreSessions)
			documentCount = len(topOgreSessions)
		}

		if err != nil {
			panic(err)
		}
		fmt.Println("Retrieved", documentCount, teamName, "sessions!")
	}

	return topHumanSessions, topOgreSessions
}

func sortSessionsIntoHumansAndOgres(unsorted []lib.GameSession) (humans, ogres []lib.GameSession) {
	for _, value := range unsorted {
		switch value.Team {
		case "humans":
			humans = append(humans, value)
		case "ogres":
			ogres = append(ogres, value)
		}
	}
	return humans, ogres
}
func generateAllSessionPairs(humans, ogres []lib.GameSession) []lib.GameSessionPair {
	var allSessionPairs []lib.GameSessionPair

	for _, humanSession := range humans {
		for _, ogreSession := range ogres {
			allSessionPairs = append(allSessionPairs, lib.GameSessionPair{humanSession, ogreSession})
		}
	}
	return allSessionPairs
}

func insertPairsIntoRedisQueue(pairs []lib.GameSessionPair, redisConnection redis.Conn) {
	redisConnection.Send("MULTI")
	for _, pair := range pairs {
		redisConnection.Send("SADD", lib.UnprocessedSetName, pair.RedisQueueKey())
	}
	redisConnection.Send("SDIFFSTORE", lib.UnprocessedSetName, lib.UnprocessedSetName, lib.ProcessedSetName)
	_, err := redisConnection.Do("EXEC")
	if err != nil {
		panic(err)
	}
}
func main() {
	c := ConnectToMongoAndGetCollection()
	humans, ogres := GetAllRelevantSessions(c)
	allSessionPairs := generateAllSessionPairs(humans, ogres)
	fmt.Println("Generated", len(allSessionPairs), "session pairs!")
	r := lib.ConnectToRedis()
	insertPairsIntoRedisQueue(allSessionPairs, r)
}
