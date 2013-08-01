package main

import (
	"../../dynago"
	"code.google.com/p/gcfg"

	"fmt"
	"log"
	"os"
)

// the configuration should look like the following
// (with multiple profiles and a selected one)
//
// [dynago]
// profile=xxx
//
// [profile "xxx"]
// region=us-west-1
// accessKey=XXXXXXXX
// secretKey=YYYYYYYY

type Config struct {
	Dynago struct {
		// define default profile
		Profile string
	}

	// list of named profiles
	Profile map[string]*struct {
		Region    string
		AccessKey string
		SecretKey string
	}
}

func main() {
	var config Config

	err := gcfg.ReadFileInto(&config, ".dynagorc")
	if err != nil {
		log.Fatal(err)
	}

	selected := config.Dynago.Profile

	if len(os.Args) > 1 {
		// there is at least one parameter:
		// override the selected profile
		selected = os.Args[1]
	}

	profile := config.Profile[selected]

	db := dynago.NewDBClient()

	if len(profile.Region) > 0 {
		db.WithRegion(profile.Region)
	}

	if len(profile.AccessKey) > 0 {
		db.WithCredentials(profile.AccessKey, profile.SecretKey)
	}

	tables, err := db.ListTables()

	if err != nil {
		log.Fatal(err)
	}

	for _, tableName := range tables {
		table, err := db.DescribeTable(tableName)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(table.TableName)
		fmt.Println("  created:", table.CreationDateTime)
		fmt.Println("  rc:", table.ProvisionedThroughput.ReadCapacityUnits,
			"wc:", table.ProvisionedThroughput.WriteCapacityUnits)
		fmt.Println("  items:", table.ItemCount)
		fmt.Println("")
	}
}
