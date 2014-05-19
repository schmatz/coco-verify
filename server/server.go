package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/ec2"
	"github.com/schmatz/coco-verify/lib"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type SpotRequestResp ec2.SpotRequestsResp

func connectToEC2() *ec2.EC2 {
	auth, err := aws.GetAuth("***REMOVED***", "***REMOVED***")
	if err != nil {
		panic(err)
	}
	return ec2.New(auth, aws.USEast)
}

func getSpotInstanceRequests(e *ec2.EC2) []ec2.SpotRequestResult {
	spotRequests, err := e.DescribeSpotRequests(nil, nil)
	if err != nil {
		panic(err)
	}
	return spotRequests.SpotRequestResults
}

func connectToMongoAndGetCollection() *mgo.Collection {
	connectionURL := "mongodb://" + lib.MongoUsername + ":" + lib.MongoPassword + "@" + lib.MongoURL + ":27017/" + lib.DatabaseName + "?***REMOVED***"
	mongoSession, err := mgo.Dial(connectionURL)
	if err != nil {
		panic(err)
	}
	c := mongoSession.DB(lib.DatabaseName).C(lib.CollectionName)
	fmt.Println("Connected to collection", lib.CollectionName)
	return c
}

func getAllRelevantSessions(levelSessionsCollection *mgo.Collection) []lib.GameSession {
	var gameSessions []lib.GameSession
	queryParameters := bson.M{"level.original": "***REMOVED***", "submitted": true}
	selection := bson.M{"team": 1}
	err := levelSessionsCollection.Find(queryParameters).Select(selection).All(&gameSessions)
	if err != nil {
		panic(err)
	}
	fmt.Println("Retrieved", len(gameSessions), "sessions to verify!")
	return gameSessions
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
	c := connectToMongoAndGetCollection()
	unprocessedSessions := getAllRelevantSessions(c)
	humans, ogres := sortSessionsIntoHumansAndOgres(unprocessedSessions)
	allSessionPairs := generateAllSessionPairs(humans, ogres)
	fmt.Println("Generated", len(allSessionPairs), "session pairs!")
	r := lib.ConnectToRedis()
	insertPairsIntoRedisQueue(allSessionPairs, r)
	e := connectToEC2()
	getSpotInstanceRequests(e)

}
