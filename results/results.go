package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/schmatz/coco-verify/lib"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"sort"
)

type MongoCreatorName struct {
	CreatorName string `bson:"creatorName" json:"creatorName"`
	Playtime    int    `bson:"playtime" json:"playtime"`
	Email       string `bson:"emailLower" json:"emailLower"`
}

type GameSessionResult struct {
	Session         lib.GameSession
	Wins            int
	Losses          int
	CreatorName     string
	Email           string
	PreliminaryRank int
	Playtime        int
}

type By func(s1, s2 *GameSessionResult) bool

func (rs *resultSorter) Sort(results []GameSessionResult) {
	rs.results = results
	sort.Sort(rs)

}

type resultSorter struct {
	results []GameSessionResult
	less    []By
}

func OrderedBy(less ...By) *resultSorter {
	return &resultSorter{
		less: less,
	}
}

func (s *resultSorter) Len() int {
	return len(s.results)
}
func (s *resultSorter) Swap(i, j int) {
	s.results[i], s.results[j] = s.results[j], s.results[i]
}
func (s *resultSorter) Less(i, j int) bool {
	p, q := &s.results[i], &s.results[j]
	var k int
	for k = 0; k < len(s.less)-1; k++ {
		less := s.less[k]
		switch {
		case less(p, q):
			return true
		case less(q, p):
			return false
		}
	}
	return s.less[k](p, q)
}

//shameless copying, perhaps put into a library later
func ConnectToMongoAndGetCollection() (c *mgo.Collection, m *mgo.Session) {
	connectionURL := "mongodb://" + lib.MongoUsername + ":" + lib.MongoPassword + "@" + lib.MongoURL + ":27017/" + lib.DatabaseName + "?authSource=admin"
	mongoSession, err := mgo.Dial(connectionURL)
	if err != nil {
		panic(err)
	}
	c = mongoSession.DB(lib.DatabaseName).C(lib.CollectionName)
	fmt.Println("Connected to collection", lib.CollectionName)
	return c, mongoSession
}

func GetAllRelevantSessions(levelSessionsCollection *mgo.Collection) []lib.GameSession {
	var gameSessions []lib.GameSession
	queryParameters := bson.M{"level.original": "53558b5a9914f5a90d7ccddb", "submitted": true}
	selection := bson.M{"team": 1, "creator": 1}
	err := levelSessionsCollection.Find(queryParameters).Select(selection).All(&gameSessions)
	if err != nil {
		panic(err)
	}
	fmt.Println("Retrieved", len(gameSessions), "sessions to verify!")
	return gameSessions
}

func getWinsAndLosses(s lib.GameSession, r redis.Conn) GameSessionResult {
	var result GameSessionResult
	result.Session = s
	wins, err := redis.Int(r.Do("SCARD", result.Session.GetWinningRedisKey()))
	if err != nil {
		panic(err)
	}
	result.Wins = wins
	losses, err := redis.Int(r.Do("SCARD", result.Session.GetLosingRedisKey()))
	if err != nil {
		panic(err)
	}
	result.Losses = losses
	return result
}

func (r GameSessionResult) getCreatorName(c *mgo.Collection) GameSessionResult {
	creatorName := MongoCreatorName{}
	selection := bson.M{"creatorName": 1, "playtime": 1}
	err := c.FindId(r.Session.ID).Select(selection).One(&creatorName)
	if err != nil {
		panic(err)
	}
	r.CreatorName = string(creatorName.CreatorName)
	r.Playtime = int(creatorName.Playtime)
	return r
}

func (r GameSessionResult) getSessionDetails(m *mgo.Session) GameSessionResult {
	sessionDetails := MongoCreatorName{}
	selection := bson.M{"emailLower": 1}
	c := m.DB(lib.DatabaseName).C("users")
	err := c.FindId(bson.ObjectIdHex(r.Session.Creator)).Select(selection).One(&sessionDetails)
	if err != nil {
		panic(err)
	}
	r.Email = string(sessionDetails.Email)
	return r
}

func main() {
	c, m := ConnectToMongoAndGetCollection()
	unprocessedSessions := GetAllRelevantSessions(c)
	r := lib.ConnectToRedis()
	var results []GameSessionResult
	for _, session := range unprocessedSessions {
		results = append(results, getWinsAndLosses(session, r))
	}

	increasingWins := func(r1, r2 *GameSessionResult) bool {
		return (r1.Wins - r1.Losses) > (r2.Wins - r2.Losses)
	}
	OrderedBy(increasingWins).Sort(results)
	for i, result := range results {
		results[i] = result.getCreatorName(c)
	}
	for i, result := range results {
		results[i] = result.getSessionDetails(m)
	}
	var ogres []GameSessionResult
	var humans []GameSessionResult
	for _, result := range results {
		if result.Session.Team == "ogres" {
			ogres = append(ogres, result)
		} else {
			humans = append(humans, result)
		}
	}

	fmt.Println("Top ogres")
	for _, result := range ogres {
		fmt.Printf("%d,%s,%s,%s,%d,%d,%d\n", result.PreliminaryRank, bson.ObjectId(result.Session.ID), result.CreatorName, result.Email, result.Playtime, result.Wins, result.Losses)
	}
	fmt.Println("Top humans")
	for _, result := range humans {
		fmt.Printf("%d,%s,%s,%s,%d,%d,%d\n", result.PreliminaryRank, bson.ObjectId(result.Session.ID), result.CreatorName, result.Email, result.Playtime, result.Wins, result.Losses)
	}

}
