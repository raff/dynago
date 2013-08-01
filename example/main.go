package main

import (
	"../../dynago"

	"fmt"
	"log"
)

func main() {
	// use all defaults
	// db := dynago.NewDBClient()

	// set region
	//db := dynago.NewDBClient().
	//        WithRegion(dynago.REGION_US_WEST_1)

	// set region and credentials
	db := dynago.NewDBClient().
		WithRegion(dynago.REGION_US_EAST_1).
		WithCredentials("YOURACCESSKEY", "YOURSECRETKEY")

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
