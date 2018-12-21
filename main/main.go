package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/olivere/elastic"
	"google.golang.org/api/option"
)

const (
	POST_INDEX = "post"
	POST_TYPE  = "post"
	DISTANCE   = "200km"

	ES_URL          = "http://35.230.14.141:9200/"
	BUCKET_NAME     = "discovery-post-images"
	CREDENTIAL_PATH = "../credentials/Discovery-0e775b4e419c.json"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
}

func main() {
	createIndexIfNotExists()
	http.HandleFunc("/post", postHandler)
	http.HandleFunc("/search", searchHandler)
	fmt.Println("started-service")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func createIndexIfNotExists() {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(POST_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !exists {
		mapping := `{
			"mappings": {
				"post": {
					"properties": {
						"location": {
							"type": "geo_point"
						}
					}
				}
			}
		}`
		_, err = client.CreateIndex(POST_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
}

func saveToGCS(reader io.Reader, bucketName, objectName string) (*storage.ObjectAttrs, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(CREDENTIAL_PATH))
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)
	if _, err = bucket.Attrs(ctx); err != nil {
		return nil, err
	}

	object := bucket.Object(objectName)
	writer := object.NewWriter(ctx)
	if _, err = io.Copy(writer, reader); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}

	if err = object.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, err
	}

	attrs, err := object.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Image is saved to GCS: %s\n", attrs.MediaLink)
	return attrs, nil
}

func saveToES(post *Post, id string) error {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return err
	}
	_, err = client.Index().
		Index(POST_INDEX).
		Type(POST_TYPE).
		Id(id).
		BodyJson(post).
		Refresh("wait_for").
		Do(context.Background())
	if err != nil {
		return err
	}
	fmt.Printf("Post is saved to index: %s\n", post.Message)
	return nil
}

func readFromES(lat, lon float64, ran string) ([]Post, error) {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}

	query := elastic.NewGeoDistanceQuery("location")
	query = query.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().
		Index(POST_INDEX).
		Query(query).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ptyp Post
	var posts []Post
	for _, item := range searchResult.Each(reflect.TypeOf(ptyp)) {
		if p, ok := item.(Post); ok {
			posts = append(posts, p)
		}
	}

	return posts, nil
}

func postHandler(writer http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one post request.")

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	lat, _ := strconv.ParseFloat(req.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(req.FormValue("lon"), 64)

	img, _, err := req.FormFile("image")
	if err != nil {
		http.Error(writer, "Image is not available", http.StatusBadRequest)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}

	id := uuid.New().String()
	attrs, err := saveToGCS(img, BUCKET_NAME, id)
	if err != nil {
		http.Error(writer, "Failed to save image to GCS", http.StatusInternalServerError)
		fmt.Printf("Failed to save image to GCS %v.\n", err)
		return
	}

	post := &Post{
		User:    req.FormValue("user"),
		Message: req.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
		Url: attrs.MediaLink,
	}

	err = saveToES(post, id)
	if err != nil {
		http.Error(writer, "Failed to save post to ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to save post to ElasticSearch %v.\n", err)
		return
	}
	fmt.Printf("Saved one post to ElasticSearch: %s\n", post.Message)

}

func searchHandler(writer http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one request for search")

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	lat, _ := strconv.ParseFloat(req.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(req.URL.Query().Get("lon"), 64)
	// range is optional
	ran := DISTANCE
	if val := req.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	posts, err := readFromES(lat, lon, ran)
	if err != nil {
		http.Error(writer, "Failed to read post from ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to read post from ElasticSearch %v.\n", err)
		return
	}

	js, err := json.Marshal(posts)
	if err != nil {
		http.Error(writer, "Failed to parse posts into JSON format", http.StatusInternalServerError)
		fmt.Printf("Failed to parse posts into JSON format %v.\n", err)
		return
	}

	writer.Write(js)
}
