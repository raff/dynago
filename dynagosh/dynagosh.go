//
// An interactive shell for DynamoDB
//
package main

import (
	"../../dynago"

	"code.google.com/p/gcfg"
	"github.com/gobs/args"
	"github.com/gobs/cmd"
	"github.com/gobs/pretty"

	"fmt"
	"log"
	"os"
	"path"
	"strings"
)

const (
	CONFIG_FILE  = ".dynagorc"
	HISTORY_FILE = ".dynago_history"
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

// Look for configFile in current directory or home directory.
// No configuration file is NOT an error.
// A malformed configuration file is a FATAL error.

func ReadConfig(configFile string, config *Config) {
	// configFile in current directory or full path
	if _, err := os.Stat(configFile); err != nil {
		if strings.Contains(configFile, "/") {
			return
		}

		// configFile in home directory
		configFile = path.Join(os.Getenv("HOME"), configFile)
		if _, err := os.Stat(configFile); err != nil {
			return
		}
	}

	err := gcfg.ReadFileInto(config, configFile)
	if err != nil {
		log.Fatal(err)
	}
}

var (
	// this hold the current list of table names, to be used by the CompletionFunction
	table_list []string
)

func add_to_list(table string) {
	table_list = append(table_list, table)
}

func remove_from_list(table string) {
	for i, t := range table_list {
		if t == table {
			table_list = append(table_list[:i], table_list[i+1:]...)
			return
		}
	}
}

// return list of table names that match the input pattern (table name starts with "text")
func CompletionFunction(text string, line string, start, stop int) []string {
	if len(table_list) > 0 {
		matches := make([]string, 0, len(table_list))

		for _, w := range table_list {
			if strings.HasPrefix(w, text) {
				matches = append(matches, w)
			}
		}

		return matches
	}

	return nil
}

func main() {
	var config Config
	ReadConfig(CONFIG_FILE, &config)

	selected := config.Dynago.Profile

	if len(os.Args) > 1 {
		// there is at least one parameter:
		// override the selected profile
		selected = os.Args[1]
	}

	profile := config.Profile[selected]
	if profile == nil {
		log.Fatal("no profile selected")
	}

	db := dynago.NewDBClient()

	if len(profile.Region) > 0 {
		db.WithRegion(profile.Region)
	}

	if len(profile.AccessKey) > 0 {
		db.WithCredentials(profile.AccessKey, profile.SecretKey)
	}

	commander := &cmd.Cmd{HistoryFile: HISTORY_FILE, Complete: CompletionFunction, EnableShell: true}
	commander.Prompt = "dynagosh> "
	commander.Init()

	commander.Add(cmd.Command{"config",
		`
		config : display current configuration
		`,
		func(string) (stop bool) {
			pretty.PrettyPrint(config)
			return
		}})

	commander.Add(cmd.Command{"list",
		`
                list : display list of available tables
                `,
		func(string) (stop bool) {
			tables, err := db.ListTables()

			if err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println("Available tables")

			for _, tableName := range tables {
				fmt.Println("  ", tableName)
			}

			if len(tables) > 0 {
				table_list = tables
			}

			return
		}})

	commander.Add(cmd.Command{"describe",
		`
                describe {table} : display table configuration
                `,
		func(line string) (stop bool) {
			tableName := line
			table, err := db.DescribeTable(tableName)
			if err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(table)
			}

			return
		}})

	commander.Add(cmd.Command{"create",
		`
		create {tablename} hashKey:hashType [rangeKey:rangeType] [readCapacity] [writeCapacity]
		`,
		func(line string) (stop bool) {
			args := args.GetArgs(line)

			if len(args) < 2 {
				fmt.Println("not enough arguments")
				return
			}

			tableName := args[0]

			hashKey := &dynago.AttributeDefinition{AttributeType: dynago.STRING_ATTRIBUTE}
			var rangeKey *dynago.AttributeDefinition
			rc := 5
			wc := 5

			if strings.Contains(args[1], ":") {
				parts := strings.Split(args[1], ":")
				hashKey.AttributeName = parts[0]
				hashKey.AttributeType = parts[1]
			} else {
				hashKey.AttributeName = args[1]
			}

			if len(args) > 2 {
				rangeKey := &dynago.AttributeDefinition{AttributeType: dynago.STRING_ATTRIBUTE}

				if strings.Contains(args[2], ":") {
					parts := strings.Split(args[2], ":")
					rangeKey.AttributeName = parts[0]
					rangeKey.AttributeType = parts[1]
				} else {
					rangeKey.AttributeName = args[2]
				}
			}

			if table, err := db.CreateTable(tableName, hashKey, rangeKey, rc, wc); err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(table)
				add_to_list(tableName)
			}

			return
		}})

	commander.Add(cmd.Command{"delete",
		`
                delete {table} : delete table
                `,
		func(line string) (stop bool) {
			tableName := line
			table, err := db.DeleteTable(tableName)
			if err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(table)
				remove_from_list(tableName)
			}

			return
		}})

	commander.Commands["ls"] = commander.Commands["list"]
	commander.Commands["drop"] = commander.Commands["delete"]

	commander.CmdLoop()
}
