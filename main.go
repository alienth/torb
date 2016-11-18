package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var cass *gocql.Session

func main() {
	var err error
	cass, err = cassSession()
	if err != nil {
		log.Fatal(err)
	}
	http.HandleFunc("/api/put", put)

	http.ListenAndServe(":8080", nil)
}

type unixTime struct{ time.Time }

func (u *unixTime) UnmarshalJSON(b []byte) error {
	ts, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}

	u.Time = time.Unix(int64(ts), 0)
	return nil
}

//type Tags map[string]string
//
//func (t Tags) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
//	var result string
//	for k, v := range t {
//		result += fmt.Sprintf("%s=%s:", k, v)
//	}
//
//	return []byte(result), nil
//
//}

type putRequest struct {
	Metric    string
	Value     string
	Timestamp unixTime
	Offset    int
	Tags      map[string]string
}

func put(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)

	var req putRequest

	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&req)
	if err != nil {
		fmt.Println(err)
	}

	rowTime := calculateRowTime(req.Timestamp.Time)
	offset := req.Timestamp.Time.Sub(rowTime)

	if err := cass.Query(`INSERT INTO tsdb (metric, tags, time, offset, value) VALUES (?, ?, ?, ?, ?)`,
		req.Metric,
		req.Tags,
		rowTime,
		offset,
		req.Value).Exec(); err != nil {
		fmt.Println(err)
	}

	fmt.Println(req)
}

func timestampParser(ts string) (time.Time, error) {
	var result time.Time
	if strings.Contains(ts, "-ago") {
		ts = strings.Split(ts, "-")[0]
		suffix := ts[len(ts)-1:]
		var duration time.Duration
		switch suffix {
		case "s":
			duration = time.Second
		case "h":
			duration = time.Hour
		case "w":
			duration = time.Hour * 24 * 7
		case "n":
			duration = time.Hour * 24 * 7 * 30
		case "y":
			duration = time.Hour * 24 * 365
		default:
			return time.Now(), fmt.Errorf("Unknown time suffix: %s\n", suffix)
		}
		prefix, err := strconv.Atoi(ts[:len(ts)-1])
		if err != nil {
			return time.Now(), err
		}
		result = time.Now().Add(time.Duration(prefix) * duration * -1)
	}

	return result, nil
}

type filterType string

type tagFilter struct {
	Type    filterType
	Tagk    string
	Filter  string
	GroupBy bool
}

type aggregatorType string

type query struct {
	Aggregator aggregatorType
	Downsample string
	Metric     string
	Rate       bool
	Filters    []tagFilter
}

type queryRequest struct {
	Start   string
	End     string
	Queries []query
}

const ROW_WIDTH = time.Hour * 24 * 7 * 3

func calculateRowTime(t time.Time) time.Time {
	return t.Truncate(ROW_WIDTH)
}

func (q queryRequest) Parse() {
	//	q.End = timestampParser(q.End)
	//	q.Start = timestampParser(q.Start)
}

func apiQuery(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)

	var req queryRequest

	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&req)
	if err != nil {
		fmt.Println(err)
	}

	//	queryRequest.Parse()

	//	if err := cass.Query(`SELECT offset FROM tsdb WHERE metric=? AND tags=? AND `,
	//		req.Metric,
	//		req.Tags,
	//		req.Timestamp.Time,
	//		req.Offset,
	//		req.Value).Exec(); err != nil {
	//		fmt.Println(err)
	//	}

	fmt.Println(req)
}

// create keyspace torb  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// create table tsdb ( metric text,  time timestamp, tags frozen <map<text, text>>, offset int, value blob, PRIMARY KEY (metric, time, tags) );
func cassSession() (*gocql.Session, error) {
	cluster := gocql.NewCluster("172.17.0.2")
	cluster.Keyspace = "torb"

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return session, nil

	//	if err := session.Query(`INSERT INTO tsdb (metric, tags, time, offset, value) VALUES (?, ?, ?, ?, ?)`, "test", "host=ny-jharvey01", time.Now(), 123, "0").Exec(); err != nil {
	//		fmt.Println(err)
	//	}
}
