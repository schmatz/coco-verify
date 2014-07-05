package main

import (
	"fmt"
	"github.com/schmatz/coco-verify/lib"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type Simulator struct {
	Calculator bson.ObjectId `bson:"calculator,omitempty"`
	Count      int
}

func ConnectToMongoAndGetCollection() *mgo.Collection {
	connectionURL := "mongodb://" + lib.MongoUsername + ":" + lib.MongoPassword + "@" + lib.MongoURL + ":27017/" + lib.DatabaseName + "?authSource=admin"
	mongoSession, err := mgo.Dial(connectionURL)
	if err != nil {
		panic(err)
	}
	c := mongoSession.DB(lib.DatabaseName).C("scoringtasks")
	fmt.Println("Connected to collection", "scoringtasks")
	return c
}

func GetAllRelevantSessions(scoringTaskCollection *mgo.Collection) []Simulator {
	var calculators []Simulator
	err := scoringTaskCollection.Find(bson.M{}).Distinct("calculator", &calculators)
	if err != nil {
		panic(err)
	}
	fmt.Println(calculators)
	return calculators
}

func main() {
	c := ConnectToMongoAndGetCollection()
	GetAllRelevantSessions(c)
}
