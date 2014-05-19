package lib

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo/bson"
)

const DatabaseName string = "coco"
const CollectionName string = "level.sessions"
const MongoURL string = "***REMOVED***"
const MongoUsername string = "***REMOVED***"
const MongoPassword string = "***REMOVED***"
const RedisPassword string = "***REMOVED***"
const RedisHost string = "***REMOVED***:6379"
const UnprocessedSetName string = "unprocessed"
const ProcessedSetName string = "processed"
const ProcessingName string = "processing"

type GameSession struct {
	ID   bson.ObjectId `bson:"_id,omitempty"`
	Team string
}

type GameSessionPair [2]GameSession

func (g GameSessionPair) RedisQueueKey() string {
	return g[0].ID.Hex() + g[1].ID.Hex()
}
func (g *GameSession) GetWinningRedisKey() string {
	return g.ID.Hex() + "w"
}
func (g *GameSession) GetLosingRedisKey() string {
	return g.ID.Hex() + "l"
}

func ConnectToRedis() redis.Conn {
	redisConnection, err := redis.Dial("tcp", RedisHost)
	if err != nil {
		panic(err)
	}
	isAuth, err := redisConnection.Do("AUTH", RedisPassword)
	if isAuth != "OK" || err != nil {
		panic("Redis authentication failed!")
	}
	fmt.Println("Connected to Redis!")
	return redisConnection
}
