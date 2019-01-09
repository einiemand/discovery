package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/olivere/elastic"
)

const (
	USER_INDEX = "user"
	USER_TYPE  = "user"
)

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Age      int64  `json:"age"`
	Gender   string `json:"gender"`
}

var mySigningKey = []byte("secret")

func verifyUser(username, password string) error {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return err
	}

	// select * from users where username = ?
	query := elastic.NewTermQuery("username", username)

	searchResult, err := client.Search().Index(USER_INDEX).Query(query).Pretty(true).Do(context.Background())
	if err != nil {
		return err
	}
	var utype User
	for _, item := range searchResult.Each(reflect.TypeOf(utype)) {
		if u, ok := item.(User); ok {
			if username == u.Username && password == u.Password {
				fmt.Println("Login as " + username)
				return nil
			}
		}
	}
	return errors.New("Invalid username or password")
}

func registerUser(user User) error {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return err
	}

	query := elastic.NewTermQuery("username", user.Username)

	searchResult, err := client.Search().Index(USER_INDEX).Query(query).Pretty(true).Do(context.Background())
	if err != nil {
		return err
	}

	if searchResult.TotalHits() > 0 {
		return errors.New("Username already exists")
	}

	_, err = client.Index().Index(USER_INDEX).Type(USER_TYPE).Id(user.Username).BodyJson(user).Refresh("wait_for").Do(context.Background())
	if err != nil {
		return err
	}

	fmt.Println("Successfully registered user: " + user.Username)
	return nil
}
