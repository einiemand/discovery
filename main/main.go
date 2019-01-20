package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	jwtmiddleware "github.com/auth0/go-jwt-middleware"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/olivere/elastic"
	"google.golang.org/api/option"
)

const (
	POST_INDEX = "post"
	POST_TYPE  = "post"
	DISTANCE   = "200km"

	ES_URL          = "http://35.230.45.198:9200/"
	BUCKET_NAME     = "discovery-post-images"
	CREDENTIAL_PATH = "Discovery-0e775b4e419c.json"

	PROJECT_ID      = "discovery-225722"
	BT_INSTANCE     = "discovery-post"
	ENABLE_BIGTABLE = false

	API_PREFIX = "/api/v1"
)

var (
	mediaTypes = map[string]string{
		".jpeg": "image",
		".jpg":  "image",
		".gif":  "image",
		".png":  "image",
		".mov":  "video",
		".mp4":  "video",
		".avi":  "video",
		".flv":  "video",
		".wmv":  "video",
	}
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
	Type     string   `json:"type"`
	Face     float64  `json"face"`
}

func main() {
	createIndexIfNotExists()
	jwtMiddleware := jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return []byte(mySigningKey), nil
		},
		SigningMethod: jwt.SigningMethodHS256,
	})

	r := mux.NewRouter()

	r.Handle(API_PREFIX+"/post", jwtMiddleware.Handler(http.HandlerFunc(postHandler))).Methods("POST", "OPTIONS")
	r.Handle(API_PREFIX+"/search", jwtMiddleware.Handler(http.HandlerFunc(searchHandler))).Methods("GET", "OPTIONS")
	r.Handle(API_PREFIX+"/cluster", jwtMiddleware.Handler(http.HandlerFunc(clusterHandler))).Methods("GET", "OPTIONS")

	r.Handle(API_PREFIX+"/login", http.HandlerFunc(loginHandler)).Methods("POST", "OPTIONS")
	r.Handle(API_PREFIX+"/signup", http.HandlerFunc(registerHandler)).Methods("POST", "OPTIONS")

	// Backend endpoints.
	http.Handle(API_PREFIX+"/", r)
	// Frontend endpoints.
	http.Handle("/", http.FileServer(http.Dir("build")))
	log.Fatal(http.ListenAndServe(":8080", nil))

	fmt.Println("started-service")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loginHandler(writer http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one login request")
	writer.Header().Set("Content-Type", "text/plain")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	decoder := json.NewDecoder(req.Body)
	var user User
	if err := decoder.Decode(&user); err != nil {
		http.Error(writer, "Invalid JSON format", http.StatusBadRequest)
		fmt.Printf("Invalid JSON format: %v\n", err)
		return
	}

	if err := verifyUser(user.Username, user.Password); err != nil {
		if err.Error() == "Invalid username or password" {
			http.Error(writer, err.Error(), http.StatusUnauthorized)
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": user.Username,
		"exp":      time.Now().Add(time.Minute * 10).Unix(),
	})
	tokenString, err := token.SignedString(mySigningKey)
	if err != nil {
		http.Error(writer, "Failed to generate token", http.StatusInternalServerError)
		fmt.Printf("Failed to generate token: %v\n", err)
		return
	}
	writer.Write([]byte(tokenString))
}

func registerHandler(writer http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one register request")
	writer.Header().Set("Content-Type", "text/plain")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	decoder := json.NewDecoder(req.Body)
	var user User
	if err := decoder.Decode(&user); err != nil {
		http.Error(writer, "Invalid JSON format", http.StatusBadRequest)
		fmt.Printf("Invalid JSON format: %v\n", err)
		return
	}

	if !regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString(user.Username) || user.Password == "" {
		http.Error(writer, "Wrong username or password format", http.StatusBadRequest)
		fmt.Println("Wrong username or password format")
		return
	}

	if err := registerUser(user); err != nil {
		if err.Error() == "Username already exists" {
			http.Error(writer, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	writer.Write([]byte("Successfully registered user: " + user.Username))
}

func postHandler(writer http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one post request.")

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	user := req.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

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
		http.Error(writer, "Failed to save image to GCS: "+err.Error(), http.StatusInternalServerError)
		fmt.Printf("Failed to save image to GCS %v.\n", err)
		return
	}

	im, header, _ := req.FormFile("image")
	defer im.Close()
	suffix := filepath.Ext(header.Filename)

	post := &Post{
		User:    username.(string),
		Message: req.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
		Url:  attrs.MediaLink,
		Type: "unknown",
		Face: 0,
	}

	// Client needs to know the media type so as to render it.
	if t, ok := mediaTypes[suffix]; ok {
		post.Type = t
	} else {
		post.Type = "unknown"
	}
	// ML Engine only supports jpeg.
	if suffix == ".jpeg" {
		if score, err := annotate(im); err != nil {
			http.Error(writer, "Failed to annotate the image", http.StatusInternalServerError)
			fmt.Printf("Failed to annotate the image %v\n", err)
			return
		} else {
			post.Face = score
		}
	}

	err = saveToES(post, id)
	if err != nil {
		http.Error(writer, "Failed to save post to ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to save post to ElasticSearch %v.\n", err)
		return
	}
	fmt.Printf("Saved one post to ElasticSearch: %s\n", post.Message)

	if ENABLE_BIGTABLE {
		saveToBigTable(post, id)
	}

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

func clusterHandler(writer http.ResponseWriter, req *http.Request) {
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	if req.Method == "OPTIONS" {
		return
	}

	if req.Method != "GET" {
		return
	}

	term := req.URL.Query().Get("term")

	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		http.Error(writer, "ES is not setup", http.StatusInternalServerError)
		fmt.Printf("ES is not setup %v\n", err)
		return
	}

	// Range query.
	// For details, https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
	q := elastic.NewRangeQuery(term).Gte(0.9)

	searchResult, err := client.Search().
		Index(POST_INDEX).
		Query(q).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		// Handle error
		m := fmt.Sprintf("Failed to query ES %v", err)
		fmt.Println(m)
		http.Error(writer, m, http.StatusInternalServerError)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
		p := item.(Post)
		ps = append(ps, p)

	}
	fmt.Println("length of post:", len(ps))
	js, err := json.Marshal(ps)
	if err != nil {
		m := fmt.Sprintf("Failed to parse post object %v", err)
		fmt.Println(m)
		http.Error(writer, m, http.StatusInternalServerError)
		return
	}

	writer.Write(js)
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

	exists, err = client.IndexExists(USER_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if !exists {
		_, err = client.CreateIndex(USER_INDEX).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}

}

// Save a post to BigTable
func saveToBigTable(p *Post, id string) {
	ctx := context.Background()
	bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE, option.WithCredentialsFile(CREDENTIAL_PATH))
	if err != nil {
		panic(err)
		return
	}

	tbl := bt_client.Open("post")
	mut := bigtable.NewMutation()
	t := bigtable.Now()
	mut.Set("post", "user", t, []byte(p.User))
	mut.Set("post", "message", t, []byte(p.Message))
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	err = tbl.Apply(ctx, id, mut)
	if err != nil {
		panic(err)
		return
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
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
